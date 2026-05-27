use chrono::{DateTime, Datelike, Timelike, Utc};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "redpanda.data-pipeline.svc.cluster.local:9092")]
    broker: String,

    #[arg(long, default_value = "raw-events")]
    topic: String,

    #[arg(long, default_value_t = 5)]
    tenants: usize,

    /// Target events per day across all tenants (default: 3.2M)
    #[arg(long, default_value_t = 3_200_000)]
    target_daily_events: u64,

    /// Max ratio between the quietest and busiest day (default: 3.0)
    #[arg(long, default_value_t = 3.0)]
    daily_variance: f64,

    #[arg(long)]
    seed: Option<u64>,
}

// ---------------------------------------------------------------------------
// Kafka event (identical JSON shape to the original)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Event {
    event_id: String,
    event_type: String,
    tenant_id: String,
    event_time: String,
    anonymous_id: String,
    user_id: String,
    session_id: String,
    page_url: String,
    referrer: String,
    element_id: String,
    feature_name: String,
    device_type: String,
    browser: String,
    os: String,
    country: String,
    properties: String,
}

// ---------------------------------------------------------------------------
// Static data tables
// ---------------------------------------------------------------------------

const TENANTS: &[&str] = &[
    "acme-corp",
    "globex-inc",
    "initech",
    "umbrella-co",
    "stark-ind",
    "wayne-ent",
    "oscorp",
    "lexcorp",
];

const DEVICES: &[(&str, &str, &str)] = &[
    ("desktop", "Chrome", "macOS"),
    ("desktop", "Firefox", "Windows"),
    ("desktop", "Safari", "macOS"),
    ("desktop", "Edge", "Linux"),
    ("mobile", "Safari", "iOS"),
    ("mobile", "Chrome", "Android"),
    ("tablet", "Safari", "iOS"),
    ("tablet", "Chrome", "Android"),
];
const COUNTRIES: &[&str] = &["US", "GB", "DE", "FR", "JP", "AU", "CA", "BR"];

// ---------------------------------------------------------------------------
// Behavioral constants
// ---------------------------------------------------------------------------

// Persona distribution weights (must sum to 100)
const PERSONA_POWER: u32 = 10;
const PERSONA_REGULAR: u32 = 30;
const PERSONA_CASUAL: u32 = 35;
const PERSONA_TOURIST: u32 = 25;
const _: () = assert!(PERSONA_POWER + PERSONA_REGULAR + PERSONA_CASUAL + PERSONA_TOURIST == 100);

// Session length ranges (events per session) by persona
const SESSION_LEN_POWER: (u32, u32) = (120, 300);
const SESSION_LEN_REGULAR: (u32, u32) = (50, 120);
const SESSION_LEN_CASUAL: (u32, u32) = (20, 50);
const SESSION_LEN_TOURIST: (u32, u32) = (5, 15);

// Think time median in seconds by persona (used with log-normal distribution)
const THINK_POWER: f64 = 8.0;
const THINK_REGULAR: f64 = 15.0;
const THINK_CASUAL: f64 = 25.0;
const THINK_TOURIST: f64 = 20.0;

// Conversion rates (probability of signup on first unconverted visit)
const CONVERT_POWER: f64 = 0.30;
const CONVERT_REGULAR: f64 = 0.15;
const CONVERT_CASUAL: f64 = 0.08;
const CONVERT_TOURIST: f64 = 0.02;

// Return probability base rates
const RETURN_POWER: f64 = 0.95;
const RETURN_REGULAR: f64 = 0.80;
const RETURN_CASUAL: f64 = 0.50;
const RETURN_TOURIST: f64 = 0.20;

// Return time median in hours by persona (log-normal distribution)
const RETURN_HOURS_POWER: f64 = 2.0;
const RETURN_HOURS_REGULAR: f64 = 8.0;
const RETURN_HOURS_CASUAL: f64 = 48.0; // 2 days
const RETURN_HOURS_TOURIST: f64 = 168.0; // 7 days

// Visit count decay factor for return probability
const RETURN_DECAY: f64 = 0.85;

const MAX_USERS_PER_TENANT: usize = 500_000;

// Spike parameters
const SPIKE_CHANCE_PER_HOUR: f64 = 0.10;
const SPIKE_MIN_MULTIPLIER: f64 = 5.0;
const SPIKE_MAX_MULTIPLIER: f64 = 20.0;
const SPIKE_MIN_DURATION_SECS: u64 = 900; // 15 min
const SPIKE_MAX_DURATION_SECS: u64 = 3600; // 60 min

// Diurnal curve: peak hour (UTC) and trough depth
const DIURNAL_PEAK_HOUR: f64 = 14.0;
const DIURNAL_TROUGH_FRACTION: f64 = 0.10; // trough is 10% of peak

// Page transition sentinels
const STAY: &str = "__stay__";
const BOUNCE: &str = "__bounce__";

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Persona {
    PowerUser,
    Regular,
    Casual,
    Tourist,
}

#[derive(Clone)]
struct DeviceProfile {
    device_index: u8,
    device_type: &'static str,
    browser: &'static str,
    os: &'static str,
}

enum UserState {
    InSession(usize),          // index into sessions vec
    WillReturn(DateTime<Utc>), // scheduled return time
    Churned,
}

struct User {
    tenant_id: String,
    user_number: u32,
    persona: Persona,
    devices: Vec<DeviceProfile>,
    country: &'static str,
    is_registered: bool,
    visit_count: u32,
    state: UserState,
    last_active: DateTime<Utc>,
}

struct Session {
    user_idx: usize,
    device: DeviceProfile,
    session_id: String,
    anonymous_id: String,
    signed_in: bool,
    needs_login_event: bool,
    events_until_login: Option<u32>,
    current_page: &'static str,
    previous_page: Option<&'static str>,
    next_event_at: Instant,
    events_remaining: u32,
    country: &'static str,
}

struct SpikeState {
    active: bool,
    multiplier: f64,
    ends_at: Instant,
    users_delivered: u32,
}

struct TenantState {
    name: String,
    next_user_number: u32,
    spike: SpikeState,
}

// ---------------------------------------------------------------------------
// Deterministic identity helpers (UNCHANGED from original)
// ---------------------------------------------------------------------------

fn fingerprint(tenant: &str, user_num: u32, device_idx: u8) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{tenant}:{user_num}:{device_idx}"));
    let hash = hasher.finalize();
    // Take first 16 bytes of SHA256 and format as UUID v4-compatible
    let bytes: [u8; 16] = hash[..16].try_into().unwrap();
    Uuid::from_bytes(bytes).to_string()
}

fn user_id_for(tenant: &str, user_num: u32) -> String {
    format!("user-{tenant}-{user_num:03}")
}

// ---------------------------------------------------------------------------
// Weighted random selection
// ---------------------------------------------------------------------------

fn weighted_choice<'a, T: Copy>(rng: &mut impl Rng, choices: &[(T, u32)]) -> T {
    let total: u32 = choices.iter().map(|(_, w)| w).sum();
    let mut roll = rng.random_range(0..total);
    for &(item, weight) in choices {
        if roll < weight {
            return item;
        }
        roll -= weight;
    }
    choices.last().unwrap().0
}

// ---------------------------------------------------------------------------
// Page transitions
// ---------------------------------------------------------------------------

fn next_page(rng: &mut impl Rng, current: &str, is_authenticated: bool) -> &'static str {
    let transitions: &[(&str, u32)] = if is_authenticated {
        match current {
            "/dashboard" => &[
                ("/analytics", 40),
                ("/settings", 20),
                ("/billing", 10),
                ("/docs", 5),
                (STAY, 25),
            ],
            "/analytics" => &[
                ("/dashboard", 35),
                ("/settings", 10),
                ("/billing", 5),
                ("/docs", 5),
                (STAY, 45),
            ],
            "/settings" => &[
                ("/dashboard", 40),
                ("/analytics", 15),
                ("/billing", 15),
                ("/docs", 5),
                (STAY, 25),
            ],
            "/billing" => &[
                ("/dashboard", 50),
                ("/settings", 20),
                ("/analytics", 10),
                ("/docs", 5),
                (STAY, 15),
            ],
            "/docs" => &[
                ("/dashboard", 40),
                ("/analytics", 20),
                ("/settings", 15),
                (STAY, 25),
            ],
            _ => &[
                ("/dashboard", 60),
                ("/analytics", 25),
                ("/settings", 10),
                (STAY, 5),
            ],
        }
    } else {
        match current {
            "/onboarding" => &[
                ("/docs", 30),
                ("/dashboard", 35),
                ("/analytics", 5),
                (BOUNCE, 30),
            ],
            "/docs" => &[
                ("/onboarding", 20),
                ("/dashboard", 40),
                ("/analytics", 10),
                (BOUNCE, 30),
            ],
            "/dashboard" => &[
                ("/analytics", 40),
                ("/settings", 15),
                ("/billing", 10),
                ("/docs", 10),
                (STAY, 25),
            ],
            "/analytics" => &[
                ("/dashboard", 35),
                ("/settings", 10),
                ("/billing", 5),
                ("/docs", 5),
                (STAY, 45),
            ],
            "/settings" => &[
                ("/dashboard", 40),
                ("/analytics", 15),
                ("/billing", 15),
                (STAY, 30),
            ],
            "/billing" => &[
                ("/dashboard", 50),
                ("/settings", 20),
                ("/analytics", 10),
                (STAY, 20),
            ],
            _ => &[("/onboarding", 40), ("/docs", 30), (BOUNCE, 30)],
        }
    };
    weighted_choice(rng, transitions)
}

// ---------------------------------------------------------------------------
// Stay event type (contextual action on current page)
// ---------------------------------------------------------------------------

fn stay_event_type(rng: &mut impl Rng, page: &str) -> &'static str {
    match page {
        "/onboarding" => "click",
        "/dashboard" => weighted_choice(rng, &[("click", 55), ("feature_used", 45)]),
        "/analytics" => weighted_choice(rng, &[("click", 45), ("feature_used", 55)]),
        "/settings" => weighted_choice(rng, &[("click", 50), ("feature_used", 50)]),
        "/billing" => weighted_choice(rng, &[("click", 80), ("feature_used", 20)]),
        "/docs" => weighted_choice(rng, &[("click", 85), ("feature_used", 15)]),
        _ => "click",
    }
}

// ---------------------------------------------------------------------------
// Feature selection per page
// ---------------------------------------------------------------------------

fn feature_for_page(rng: &mut impl Rng, page: &str) -> &'static str {
    match page {
        "/analytics" => weighted_choice(rng, &[("export_csv", 80), ("dark_mode", 20)]),
        "/settings" => weighted_choice(
            rng,
            &[
                ("dark_mode", 30),
                ("api_keys", 30),
                ("sso_login", 25),
                ("team_invite", 15),
            ],
        ),
        "/dashboard" => weighted_choice(
            rng,
            &[("export_csv", 40), ("dark_mode", 30), ("team_invite", 30)],
        ),
        "/billing" => weighted_choice(rng, &[("team_invite", 70), ("api_keys", 30)]),
        "/docs" => weighted_choice(rng, &[("dark_mode", 60), ("api_keys", 40)]),
        _ => "dark_mode",
    }
}

// ---------------------------------------------------------------------------
// Diurnal traffic curve
// ---------------------------------------------------------------------------

/// Returns a multiplier based on hour of day with per-hour randomisation.
/// Base shape is a raised cosine peaking at DIURNAL_PEAK_HOUR, normalized
/// so the 24-hour average is 1.0. Each call adds ±15% jitter so the curve
/// isn't a perfect sine — some afternoons are hotter, some mornings sluggish.
fn diurnal_multiplier(rng: &mut impl Rng, hour_fractional: f64) -> f64 {
    let delta = (hour_fractional - DIURNAL_PEAK_HOUR).rem_euclid(24.0);
    let phase = if delta > 12.0 { delta - 24.0 } else { delta };
    let cos_val = (phase * std::f64::consts::PI / 12.0).cos();
    let raw = DIURNAL_TROUGH_FRACTION + (1.0 - DIURNAL_TROUGH_FRACTION) * (1.0 + cos_val) / 2.0;
    let mean = (1.0 + DIURNAL_TROUGH_FRACTION) / 2.0;
    let base = raw / mean;
    let jitter = 1.0 + rng.random_range(-0.15..0.15);
    (base * jitter).max(0.05)
}

// ---------------------------------------------------------------------------
// Daily drift (Ornstein-Uhlenbeck process for day-to-day variance)
// ---------------------------------------------------------------------------

struct DailyDrift {
    log_multiplier: f64,
    log_half_range: f64, // ln(sqrt(daily_variance))
    mean_reversion: f64,
    last_update_hour: u32,
}

impl DailyDrift {
    fn new(daily_variance: f64) -> Self {
        Self {
            log_multiplier: 0.0,
            log_half_range: (daily_variance.sqrt()).ln(),
            mean_reversion: 0.03,
            last_update_hour: u32::MAX,
        }
    }

    fn update(&mut self, rng: &mut impl Rng, current_hour: u32) {
        if current_hour == self.last_update_hour {
            return;
        }
        self.last_update_hour = current_hour;
        let noise: f64 = rng.random_range(-1.0..1.0) * self.log_half_range * 0.08;
        self.log_multiplier += -self.mean_reversion * self.log_multiplier + noise;
        self.log_multiplier = self.log_multiplier.clamp(-self.log_half_range, self.log_half_range);
    }

    fn multiplier(&self) -> f64 {
        self.log_multiplier.exp()
    }
}

// ---------------------------------------------------------------------------
// Think time (log-normal distribution)
// ---------------------------------------------------------------------------

fn think_time(rng: &mut impl Rng, persona: Persona) -> Duration {
    let median = match persona {
        Persona::PowerUser => THINK_POWER,
        Persona::Regular => THINK_REGULAR,
        Persona::Casual => THINK_CASUAL,
        Persona::Tourist => THINK_TOURIST,
    };
    let ln_median = median.ln();
    let sigma = 0.6;
    let dist = LogNormal::new(ln_median, sigma).unwrap();
    let secs: f64 = dist.sample(rng);
    Duration::from_secs_f64(secs.clamp(2.0, 120.0))
}

// ---------------------------------------------------------------------------
// User creation
// ---------------------------------------------------------------------------

fn create_user(rng: &mut impl Rng, tenant: &str, user_number: u32) -> User {
    let roll: u32 = rng.random_range(0..100);
    let persona = if roll < PERSONA_POWER {
        Persona::PowerUser
    } else if roll < PERSONA_POWER + PERSONA_REGULAR {
        Persona::Regular
    } else if roll < PERSONA_POWER + PERSONA_REGULAR + PERSONA_CASUAL {
        Persona::Casual
    } else {
        Persona::Tourist
    };

    let device_count = {
        let d: f64 = rng.random();
        if d < 0.70 {
            1
        } else if d < 0.95 {
            2
        } else {
            3
        }
    };
    let mut used_indices: Vec<u8> = Vec::new();
    let devices: Vec<DeviceProfile> = (0..device_count)
        .map(|_| {
            let mut idx = rng.random_range(0..DEVICES.len() as u8);
            while used_indices.contains(&idx) {
                idx = rng.random_range(0..DEVICES.len() as u8);
            }
            used_indices.push(idx);
            let d = &DEVICES[idx as usize];
            DeviceProfile {
                device_index: idx,
                device_type: d.0,
                browser: d.1,
                os: d.2,
            }
        })
        .collect();

    let country = COUNTRIES[rng.random_range(0..COUNTRIES.len())];

    User {
        tenant_id: tenant.to_string(),
        user_number,
        persona,
        devices,
        country,
        is_registered: false,
        visit_count: 0,
        state: UserState::Churned,
        last_active: Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// Session creation
// ---------------------------------------------------------------------------

fn start_session(
    rng: &mut impl Rng,
    user: &mut User,
    user_idx: usize,
    sessions: &mut Vec<Session>,
) -> usize {
    user.visit_count += 1;

    let device = user.devices[rng.random_range(0..user.devices.len())].clone();
    let anonymous_id = fingerprint(&user.tenant_id, user.user_number, device.device_index);

    let country = if rng.random_bool(0.95) {
        user.country
    } else {
        COUNTRIES[rng.random_range(0..COUNTRIES.len())]
    };

    let (events_until_login, needs_login_event) = if user.is_registered {
        (None, true)
    } else {
        let rate = match user.persona {
            Persona::PowerUser => CONVERT_POWER,
            Persona::Regular => CONVERT_REGULAR,
            Persona::Casual => CONVERT_CASUAL,
            Persona::Tourist => CONVERT_TOURIST,
        };
        if rng.random_bool(rate) {
            (Some(rng.random_range(3..=10)), false)
        } else {
            (None, false)
        }
    };

    let current_page = if user.is_registered {
        weighted_choice(
            rng,
            &[
                ("/dashboard", 60),
                ("/analytics", 25),
                ("/settings", 10),
                ("/billing", 3),
                ("/docs", 2),
            ],
        )
    } else {
        weighted_choice(
            rng,
            &[
                ("/onboarding", 55),
                ("/docs", 30),
                ("/dashboard", 10),
                ("/analytics", 5),
            ],
        )
    };

    let (min, max) = match user.persona {
        Persona::PowerUser => SESSION_LEN_POWER,
        Persona::Regular => SESSION_LEN_REGULAR,
        Persona::Casual => SESSION_LEN_CASUAL,
        Persona::Tourist => SESSION_LEN_TOURIST,
    };
    let events_remaining = rng.random_range(min..=max);

    let session = Session {
        user_idx,
        device,
        session_id: Uuid::new_v4().to_string(),
        anonymous_id,
        signed_in: user.is_registered,
        needs_login_event,
        events_until_login,
        current_page,
        previous_page: None,
        next_event_at: Instant::now(),
        events_remaining,
        country,
    };

    let session_idx = sessions.len();
    sessions.push(session);
    user.state = UserState::InSession(session_idx);
    session_idx
}

// ---------------------------------------------------------------------------
// Return scheduling
// ---------------------------------------------------------------------------

fn schedule_return(rng: &mut impl Rng, user: &mut User) {
    let base_rate = match user.persona {
        Persona::PowerUser => RETURN_POWER,
        Persona::Regular => RETURN_REGULAR,
        Persona::Casual => RETURN_CASUAL,
        Persona::Tourist => RETURN_TOURIST,
    };
    let p_return = base_rate * RETURN_DECAY.powi(user.visit_count as i32);

    if rng.random_bool(p_return.clamp(0.0, 1.0)) {
        let median_hours = match user.persona {
            Persona::PowerUser => RETURN_HOURS_POWER,
            Persona::Regular => RETURN_HOURS_REGULAR,
            Persona::Casual => RETURN_HOURS_CASUAL,
            Persona::Tourist => RETURN_HOURS_TOURIST,
        };
        let dist = LogNormal::new(median_hours.ln(), 0.6).unwrap();
        let hours: f64 = dist.sample(rng);
        let hours = hours.clamp(1.0, 1440.0);
        let return_at = Utc::now() + chrono::Duration::seconds((hours * 3600.0) as i64);
        user.state = UserState::WillReturn(return_at);
    } else {
        user.state = UserState::Churned;
    }
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to create Kafka producer");

    let tenant_slice = &TENANTS[..args.tenants.min(TENANTS.len())];

    let mut rng: StdRng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_os_rng(),
    };

    let mut tenant_states: Vec<TenantState> = tenant_slice
        .iter()
        .map(|name| TenantState {
            name: name.to_string(),
            next_user_number: 1,
            spike: SpikeState {
                active: false,
                multiplier: 1.0,
                ends_at: Instant::now(),
                users_delivered: 0,
            },
        })
        .collect();

    let mut users: Vec<User> = Vec::new();
    let mut sessions: Vec<Session> = Vec::new();

    let mut total_events: u64 = 0;
    let mut last_report = Instant::now();
    let mut last_spike_check = Instant::now();
    let mut events_since_last_report: u64 = 0;
    let mut smoothed_rate: f64 = 0.0;

    // Top-down rate control: decide target events/sec, then start enough sessions
    // to sustain that rate. Sessions still clump events naturally via think times.
    let base_target_per_sec = args.target_daily_events as f64 / 86_400.0;

    // Weighted average think time across personas (used to estimate session throughput)
    let avg_think_secs: f64 = [
        (PERSONA_POWER as f64 / 100.0, THINK_POWER),
        (PERSONA_REGULAR as f64 / 100.0, THINK_REGULAR),
        (PERSONA_CASUAL as f64 / 100.0, THINK_CASUAL),
        (PERSONA_TOURIST as f64 / 100.0, THINK_TOURIST),
    ]
    .iter()
    .map(|(w, t)| w * t)
    .sum();

    let mut daily_drift = DailyDrift::new(args.daily_variance);
    let mut smoothed_target: f64 = 0.0;
    let mut effective_think_secs: f64 = avg_think_secs;
    let mut last_new_user = Instant::now();

    eprintln!(
        "Starting event producer: broker={} topic={} tenants={} target={}/day ({:.1}/s) (±{:.1}x variance)",
        args.broker, args.topic, tenant_slice.len(), args.target_daily_events, base_target_per_sec, args.daily_variance
    );

    loop {
        let now_instant = Instant::now();
        let now_utc = Utc::now();

        // === 1. Compute target rate and session budget ===
        let hour_frac = now_utc.hour() as f64 + now_utc.minute() as f64 / 60.0;
        let diurnal = diurnal_multiplier(&mut rng, hour_frac);
        daily_drift.update(&mut rng, (now_utc.date_naive().num_days_from_ce() as u32).wrapping_mul(24) + now_utc.hour());

        // Check for spike triggers (every ~60 seconds)
        if now_instant.duration_since(last_spike_check) >= Duration::from_secs(60) {
            last_spike_check = now_instant;
            for tenant_state in &mut tenant_states {
                if !tenant_state.spike.active
                    && rng.random_bool((SPIKE_CHANCE_PER_HOUR / 60.0).min(1.0))
                {
                    let multiplier = rng.random_range(SPIKE_MIN_MULTIPLIER..=SPIKE_MAX_MULTIPLIER);
                    let duration_secs =
                        rng.random_range(SPIKE_MIN_DURATION_SECS..=SPIKE_MAX_DURATION_SECS);
                    tenant_state.spike = SpikeState {
                        active: true,
                        multiplier,
                        ends_at: now_instant + Duration::from_secs(duration_secs),
                        users_delivered: 0,
                    };
                    eprintln!(
                        "[{}] SPIKE: {} {:.0}x for {}min",
                        Utc::now().format("%H:%M:%S"),
                        tenant_state.name,
                        multiplier,
                        duration_secs / 60
                    );
                }
            }
        }

        // Expire finished spikes
        for tenant_state in &mut tenant_states {
            if tenant_state.spike.active && now_instant >= tenant_state.spike.ends_at {
                eprintln!(
                    "[{}] SPIKE END: {} (delivered {} new users)",
                    Utc::now().format("%H:%M:%S"),
                    tenant_state.name,
                    tenant_state.spike.users_delivered
                );
                tenant_state.spike.active = false;
            }
        }

        // Spikes redistribute the session budget — they don't inflate it.
        // Total target stays fixed; spike tenants get a larger share.
        let target_now = base_target_per_sec * diurnal * daily_drift.multiplier();

        // Smooth the target to prevent per-tick jitter from inflating sessions.
        // Jitter creates momentary high targets that start sessions which then
        // persist through low-target ticks, biasing the average upward.
        smoothed_target = if smoothed_target == 0.0 {
            target_now
        } else {
            0.05 * target_now + 0.95 * smoothed_target
        };

        let target_sessions = (smoothed_target * effective_think_secs).round() as usize;
        let deficit = target_sessions as isize - sessions.len() as isize;

        // === 2. Start sessions to fill deficit ===
        if deficit > 0 {
            let sessions_needed = deficit as usize;

            // First, pull from returning users whose return time has arrived
            let mut started = 0usize;
            for user_idx in 0..users.len() {
                if started >= sessions_needed {
                    break;
                }
                if let UserState::WillReturn(return_at) = users[user_idx].state {
                    if now_utc >= return_at {
                        start_session(&mut rng, &mut users[user_idx], user_idx, &mut sessions);
                        started += 1;
                    }
                }
            }

            // During ramp-up (< half target sessions), fill freely.
            // At steady state, drip 1 new user every ~3 seconds (~20/min).
            let at_steady_state = sessions.len() >= target_sessions / 2;
            let can_create = !at_steady_state
                || now_instant.duration_since(last_new_user).as_secs_f64() >= 3.0;
            if started < sessions_needed && can_create {
                last_new_user = now_instant;
                let total_weight: f64 = tenant_states
                    .iter()
                    .map(|ts| if ts.spike.active { ts.spike.multiplier } else { 1.0 })
                    .sum();
                let roll: f64 = rng.random();
                let mut cumulative = 0.0;
                for tenant_state in &mut tenant_states {
                    let weight = if tenant_state.spike.active {
                        tenant_state.spike.multiplier
                    } else {
                        1.0
                    };
                    cumulative += weight / total_weight;
                    if roll < cumulative {
                        let tenant_user_count = users
                            .iter()
                            .filter(|u| u.tenant_id == tenant_state.name)
                            .count();
                        if tenant_user_count < MAX_USERS_PER_TENANT {
                            let user_num = tenant_state.next_user_number;
                            tenant_state.next_user_number += 1;
                            if tenant_state.spike.active {
                                tenant_state.spike.users_delivered += 1;
                            }
                            let mut user = create_user(&mut rng, &tenant_state.name, user_num);
                            let user_idx = users.len();
                            start_session(&mut rng, &mut user, user_idx, &mut sessions);
                            users.push(user);
                        }
                        break;
                    }
                }
            }
        }
        // When over target, returning users stay queued — they'll get
        // picked up when capacity opens up in a future tick.

        // === 4. Process sessions ready to fire ===
        let mut events_to_send: Vec<(String, String)> = Vec::new();
        let mut sessions_to_remove: Vec<usize> = Vec::new();

        for (sess_idx, session) in sessions.iter_mut().enumerate() {
            if session.next_event_at > now_instant {
                continue;
            }

            let user = &users[session.user_idx];

            // Determine event type, user_id, and page_url
            let (event_type, user_id, page_url): (&str, String, &str) = if session.needs_login_event
            {
                session.needs_login_event = false;
                session.signed_in = true;
                let think = think_time(&mut rng, user.persona);
                session.next_event_at = now_instant + think;
                (
                    "login",
                    user_id_for(&user.tenant_id, user.user_number),
                    session.current_page,
                )
            } else if session.events_remaining == 0 {
                // Session exhausted, mark for removal
                sessions_to_remove.push(sess_idx);
                continue;
            } else if let Some(ref mut countdown) = session.events_until_login {
                if *countdown == 0 {
                    // Fire signup, schedule login for next tick
                    session.events_until_login = None;
                    session.needs_login_event = true;
                    session.events_remaining -= 1;
                    let think = think_time(&mut rng, user.persona);
                    session.next_event_at = now_instant + think;
                    ("signup", String::new(), session.current_page)
                } else {
                    *countdown -= 1;
                    // Navigate or stay (anonymous, pre-conversion)
                    let navigated = next_page(&mut rng, session.current_page, session.signed_in);
                    if navigated == BOUNCE {
                        session.events_remaining = 0;
                        sessions_to_remove.push(sess_idx);
                        continue;
                    } else if navigated == STAY {
                        let et = stay_event_type(&mut rng, session.current_page);
                        session.events_remaining -= 1;
                        let think = think_time(&mut rng, user.persona);
                        session.next_event_at = now_instant + think;
                        (et, String::new(), session.current_page)
                    } else {
                        session.previous_page = Some(session.current_page);
                        session.current_page = navigated;
                        session.events_remaining -= 1;
                        let think = think_time(&mut rng, user.persona);
                        session.next_event_at = now_instant + think;
                        ("page_view", String::new(), session.current_page)
                    }
                }
            } else {
                // Regular event (authenticated or anonymous, no pending conversion)
                let navigated = next_page(&mut rng, session.current_page, session.signed_in);
                if navigated == BOUNCE {
                    session.events_remaining = 0;
                    sessions_to_remove.push(sess_idx);
                    continue;
                } else if navigated == STAY {
                    let et = stay_event_type(&mut rng, session.current_page);
                    let uid = if session.signed_in {
                        user_id_for(&user.tenant_id, user.user_number)
                    } else {
                        String::new()
                    };
                    session.events_remaining -= 1;
                    let think = think_time(&mut rng, user.persona);
                    session.next_event_at = now_instant + think;
                    (et, uid, session.current_page)
                } else {
                    session.previous_page = Some(session.current_page);
                    session.current_page = navigated;
                    let uid = if session.signed_in {
                        user_id_for(&user.tenant_id, user.user_number)
                    } else {
                        String::new()
                    };
                    session.events_remaining -= 1;
                    let think = think_time(&mut rng, user.persona);
                    session.next_event_at = now_instant + think;
                    ("page_view", uid, session.current_page)
                }
            };

            // Referrer: only meaningful for page_view and login events
            let referrer = if event_type == "page_view" || event_type == "login" {
                session.previous_page.unwrap_or("").to_string()
            } else {
                String::new()
            };

            let device = &session.device;
            let event = Event {
                event_id: Uuid::new_v4().to_string(),
                event_type: event_type.to_string(),
                tenant_id: user.tenant_id.clone(),
                event_time: Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                anonymous_id: session.anonymous_id.clone(),
                user_id,
                session_id: session.session_id.clone(),
                page_url: page_url.to_string(),
                referrer,
                element_id: if event_type == "click" {
                    format!("btn-{}", rng.random_range(1..=20))
                } else {
                    String::new()
                },
                feature_name: if event_type == "feature_used" {
                    feature_for_page(&mut rng, page_url).to_string()
                } else {
                    String::new()
                },
                device_type: device.device_type.to_string(),
                browser: device.browser.to_string(),
                os: device.os.to_string(),
                country: session.country.to_string(),
                properties: "{}".to_string(),
            };

            let tenant_id = event.tenant_id.clone();
            let json = serde_json::to_string(&event).unwrap();
            events_to_send.push((tenant_id, json));

            // Mark for removal if session exhausted and no pending login
            if session.events_remaining == 0 && !session.needs_login_event {
                sessions_to_remove.push(sess_idx);
            }
        }

        // === 5. Send events to Kafka ===
        for (tenant_id, json) in &events_to_send {
            let record = FutureRecord::to(&args.topic)
                .key(tenant_id.as_str())
                .payload(json.as_str());
            if let Err((err, _)) = producer.send(record, Duration::from_secs(1)).await {
                eprintln!("Kafka send error: {err}");
            }
            total_events += 1;
            events_since_last_report += 1;
        }

        // === 6. Clean up ended sessions, schedule returns ===
        sessions_to_remove.sort_unstable();
        sessions_to_remove.dedup();
        for &sess_idx in sessions_to_remove.iter().rev() {
            let user_idx = sessions[sess_idx].user_idx;

            if sessions[sess_idx].signed_in {
                users[user_idx].is_registered = true;
            }
            users[user_idx].last_active = Utc::now();

            schedule_return(&mut rng, &mut users[user_idx]);

            // swap_remove: fix the session index for the user whose session moved
            let last_idx = sessions.len() - 1;
            if sess_idx != last_idx {
                let moved_user_idx = sessions[last_idx].user_idx;
                if let UserState::InSession(ref mut si) = users[moved_user_idx].state {
                    *si = sess_idx;
                }
            }
            sessions.swap_remove(sess_idx);
        }

        // === 7. Status report every 10 seconds ===
        if now_instant.duration_since(last_report) >= Duration::from_secs(10) {
            let elapsed = now_instant.duration_since(last_report).as_secs_f64();
            let instant_rate = events_since_last_report as f64 / elapsed;
            smoothed_rate = if smoothed_rate == 0.0 {
                instant_rate
            } else {
                0.3 * instant_rate + 0.7 * smoothed_rate
            };
            let rate = smoothed_rate;

            // Track actual events-per-second per session to calibrate budget
            if !sessions.is_empty() && smoothed_rate > 0.0 {
                let measured = sessions.len() as f64 / smoothed_rate;
                effective_think_secs = 0.1 * measured + 0.9 * effective_think_secs;
            }

            let active_by_tenant: Vec<String> = tenant_states
                .iter()
                .map(|ts| {
                    let active = sessions
                        .iter()
                        .filter(|s| users[s.user_idx].tenant_id == ts.name)
                        .count();
                    format!(
                        "{}({})",
                        ts.name.split('-').next().unwrap_or(&ts.name),
                        active
                    )
                })
                .collect();

            let spike_info: Vec<String> = tenant_states
                .iter()
                .filter(|ts| ts.spike.active)
                .map(|ts| {
                    let remaining = ts.spike.ends_at.duration_since(now_instant).as_secs() / 60;
                    format!(
                        "{}({:.0}x, {}min left)",
                        ts.name.split('-').next().unwrap_or(&ts.name),
                        ts.spike.multiplier,
                        remaining
                    )
                })
                .collect();

            let total_users = users.len();
            let returning = users
                .iter()
                .filter(|u| matches!(u.state, UserState::WillReturn(_)))
                .count();
            let churned = users
                .iter()
                .filter(|u| matches!(u.state, UserState::Churned))
                .count();
            let registered = users.iter().filter(|u| u.is_registered).count();
            let oldest_active = users
                .iter()
                .filter(|u| !matches!(u.state, UserState::Churned))
                .map(|u| u.last_active)
                .min();
            let oldest_str = oldest_active
                .map(|t| {
                    let ago = now_utc - t;
                    if ago.num_hours() > 0 {
                        format!("{}h ago", ago.num_hours())
                    } else {
                        format!("{}m ago", ago.num_minutes())
                    }
                })
                .unwrap_or_else(|| "-".to_string());

            eprint!(
                "[{}] users={} (registered={} returning={} churned={}) sessions={} events={} rate={:.1}/s target={:.1}/s diurnal={:.2}x drift={:.2}x oldest={} | {}",
                Utc::now().format("%H:%M:%S"),
                total_users,
                registered,
                returning,
                churned,
                sessions.len(),
                total_events,
                rate,
                target_now,
                diurnal,
                daily_drift.multiplier(),
                oldest_str,
                active_by_tenant.join(" "),
            );
            if !spike_info.is_empty() {
                eprint!(" | spike: {}", spike_info.join(" "));
            }
            eprintln!();

            events_since_last_report = 0;
            last_report = now_instant;
        }

        // === 8. Sleep until next event or 1 second max ===
        let next_event_time = sessions.iter().map(|s| s.next_event_at).min();
        let sleep_until = match next_event_time {
            Some(t) if t > now_instant => t.min(now_instant + Duration::from_secs(1)),
            Some(_) => now_instant,
            None => now_instant + Duration::from_secs(1),
        };
        if sleep_until > now_instant {
            tokio::time::sleep(sleep_until - now_instant).await;
        }
    }
}
