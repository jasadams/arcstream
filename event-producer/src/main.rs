use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
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

    /// Backfill mode: generate historical events as fast as possible
    #[arg(long)]
    backfill: bool,

    /// Backfill start date (YYYY-MM-DD, default: 90 days ago)
    #[arg(long)]
    backfill_start: Option<String>,

    /// Backfill end date (YYYY-MM-DD, default: today)
    #[arg(long)]
    backfill_end: Option<String>,
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
    next_event_at: DateTime<Utc>,
    events_remaining: u32,
    country: &'static str,
}

struct SpikeState {
    active: bool,
    multiplier: f64,
    ends_at: DateTime<Utc>,
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

fn create_user(rng: &mut impl Rng, tenant: &str, user_number: u32, sim_now: DateTime<Utc>) -> User {
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
        last_active: sim_now,
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
    sim_now: DateTime<Utc>,
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
        next_event_at: sim_now,
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

fn schedule_return(rng: &mut impl Rng, user: &mut User, sim_now: DateTime<Utc>) {
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
        let return_at = sim_now + chrono::Duration::seconds((hours * 3600.0) as i64);
        user.state = UserState::WillReturn(return_at);
    } else {
        user.state = UserState::Churned;
    }
}

// ---------------------------------------------------------------------------
// Date parsing helper
// ---------------------------------------------------------------------------

fn parse_date(s: &str) -> DateTime<Utc> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap_or_else(|e| panic!("Invalid date '{s}': {e}. Use YYYY-MM-DD format."))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

// ---------------------------------------------------------------------------
// Chrono duration helper (converts std Duration to chrono Duration)
// ---------------------------------------------------------------------------

fn to_chrono(d: Duration) -> chrono::Duration {
    chrono::Duration::milliseconds(d.as_millis() as i64)
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // --- Backfill date range ---
    let backfill_end_dt = if args.backfill {
        args.backfill_end
            .as_ref()
            .map(|s| parse_date(s))
            .unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    };
    let backfill_start_dt = if args.backfill {
        args.backfill_start
            .as_ref()
            .map(|s| parse_date(s))
            .unwrap_or_else(|| backfill_end_dt - chrono::Duration::days(90))
    } else {
        Utc::now()
    };

    // --- Kafka producer (with throughput tuning for backfill) ---
    let mut producer_config = ClientConfig::new();
    producer_config
        .set("bootstrap.servers", &args.broker)
        .set("message.timeout.ms", "5000");

    if args.backfill {
        producer_config
            .set("batch.size", "1000000")
            .set("linger.ms", "100")
            .set("queue.buffering.max.messages", "1000000")
            .set("compression.type", "lz4");
    }

    let producer: FutureProducer = producer_config
        .create()
        .expect("failed to create Kafka producer");

    let tenant_slice = &TENANTS[..args.tenants.min(TENANTS.len())];

    let mut rng: StdRng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_os_rng(),
    };

    // --- Simulated clock ---
    let mut sim_now: DateTime<Utc> = if args.backfill {
        backfill_start_dt
    } else {
        Utc::now()
    };

    let mut tenant_states: Vec<TenantState> = tenant_slice
        .iter()
        .map(|name| TenantState {
            name: name.to_string(),
            next_user_number: 1,
            spike: SpikeState {
                active: false,
                multiplier: 1.0,
                ends_at: sim_now,
                users_delivered: 0,
            },
        })
        .collect();

    let mut users: Vec<User> = Vec::new();
    let mut sessions: Vec<Session> = Vec::new();

    let mut total_events: u64 = 0;
    let mut last_report = Instant::now();
    let mut last_report_sim: DateTime<Utc> = sim_now;
    let mut last_spike_check_sim: DateTime<Utc> = sim_now;
    let mut events_since_last_report: u64 = 0;
    let mut smoothed_rate: f64 = 0.0;

    let base_target_per_sec = args.target_daily_events as f64 / 86_400.0;

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
    let mut last_new_user_sim: DateTime<Utc> = sim_now;

    let backfill_real_start = Instant::now();

    if args.backfill {
        let total_days = (backfill_end_dt - backfill_start_dt).num_days();
        eprintln!(
            "Starting BACKFILL: {} to {} ({} days) broker={} topic={} tenants={} target={}/day",
            backfill_start_dt.format("%Y-%m-%d"),
            backfill_end_dt.format("%Y-%m-%d"),
            total_days,
            args.broker,
            args.topic,
            tenant_slice.len(),
            args.target_daily_events,
        );
    } else {
        eprintln!(
            "Starting event producer: broker={} topic={} tenants={} target={}/day ({:.1}/s) (±{:.1}x variance)",
            args.broker, args.topic, tenant_slice.len(), args.target_daily_events, base_target_per_sec, args.daily_variance
        );
    }

    loop {
        // --- Clock management ---
        if !args.backfill {
            sim_now = Utc::now();
        }

        if args.backfill && sim_now >= backfill_end_dt {
            break;
        }

        let real_now = Instant::now();

        // === 1. Compute target rate and session budget ===
        let hour_frac = sim_now.hour() as f64 + sim_now.minute() as f64 / 60.0;
        let diurnal = diurnal_multiplier(&mut rng, hour_frac);
        daily_drift.update(
            &mut rng,
            (sim_now.date_naive().num_days_from_ce() as u32).wrapping_mul(24) + sim_now.hour(),
        );

        // Check for spike triggers (every ~60 simulated seconds)
        if (sim_now - last_spike_check_sim).num_seconds() >= 60 {
            last_spike_check_sim = sim_now;
            for tenant_state in &mut tenant_states {
                if !tenant_state.spike.active
                    && rng.random_bool((SPIKE_CHANCE_PER_HOUR / 60.0).min(1.0))
                {
                    let multiplier =
                        rng.random_range(SPIKE_MIN_MULTIPLIER..=SPIKE_MAX_MULTIPLIER);
                    let duration_secs =
                        rng.random_range(SPIKE_MIN_DURATION_SECS..=SPIKE_MAX_DURATION_SECS);
                    tenant_state.spike = SpikeState {
                        active: true,
                        multiplier,
                        ends_at: sim_now + chrono::Duration::seconds(duration_secs as i64),
                        users_delivered: 0,
                    };
                    eprintln!(
                        "[{}] SPIKE: {} {:.0}x for {}min",
                        sim_now.format("%H:%M:%S"),
                        tenant_state.name,
                        multiplier,
                        duration_secs / 60
                    );
                }
            }
        }

        // Expire finished spikes
        for tenant_state in &mut tenant_states {
            if tenant_state.spike.active && sim_now >= tenant_state.spike.ends_at {
                eprintln!(
                    "[{}] SPIKE END: {} (delivered {} new users)",
                    sim_now.format("%H:%M:%S"),
                    tenant_state.name,
                    tenant_state.spike.users_delivered
                );
                tenant_state.spike.active = false;
            }
        }

        let target_now = base_target_per_sec * diurnal * daily_drift.multiplier();

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

            let mut started = 0usize;
            for user_idx in 0..users.len() {
                if started >= sessions_needed {
                    break;
                }
                if let UserState::WillReturn(return_at) = users[user_idx].state {
                    if sim_now >= return_at {
                        start_session(
                            &mut rng,
                            &mut users[user_idx],
                            user_idx,
                            &mut sessions,
                            sim_now,
                        );
                        started += 1;
                    }
                }
            }

            let at_steady_state = sessions.len() >= target_sessions / 2;
            let can_create = !at_steady_state
                || (sim_now - last_new_user_sim).num_seconds() >= 3;
            if started < sessions_needed && can_create {
                last_new_user_sim = sim_now;
                let total_weight: f64 = tenant_states
                    .iter()
                    .map(|ts| {
                        if ts.spike.active {
                            ts.spike.multiplier
                        } else {
                            1.0
                        }
                    })
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
                            let mut user =
                                create_user(&mut rng, &tenant_state.name, user_num, sim_now);
                            let user_idx = users.len();
                            start_session(
                                &mut rng,
                                &mut user,
                                user_idx,
                                &mut sessions,
                                sim_now,
                            );
                            users.push(user);
                        }
                        break;
                    }
                }
            }
        }

        // === 4. Process sessions ready to fire ===
        let mut events_to_send: Vec<(String, String)> = Vec::new();
        let mut sessions_to_remove: Vec<usize> = Vec::new();

        for (sess_idx, session) in sessions.iter_mut().enumerate() {
            if session.next_event_at > sim_now {
                continue;
            }

            let user = &users[session.user_idx];

            let (event_type, user_id, page_url): (&str, String, &str) = if session.needs_login_event
            {
                session.needs_login_event = false;
                session.signed_in = true;
                let think = think_time(&mut rng, user.persona);
                session.next_event_at = sim_now + to_chrono(think);
                (
                    "login",
                    user_id_for(&user.tenant_id, user.user_number),
                    session.current_page,
                )
            } else if session.events_remaining == 0 {
                sessions_to_remove.push(sess_idx);
                continue;
            } else if let Some(ref mut countdown) = session.events_until_login {
                if *countdown == 0 {
                    session.events_until_login = None;
                    session.needs_login_event = true;
                    session.events_remaining -= 1;
                    let think = think_time(&mut rng, user.persona);
                    session.next_event_at = sim_now + to_chrono(think);
                    ("signup", String::new(), session.current_page)
                } else {
                    *countdown -= 1;
                    let navigated = next_page(&mut rng, session.current_page, session.signed_in);
                    if navigated == BOUNCE {
                        session.events_remaining = 0;
                        sessions_to_remove.push(sess_idx);
                        continue;
                    } else if navigated == STAY {
                        let et = stay_event_type(&mut rng, session.current_page);
                        session.events_remaining -= 1;
                        let think = think_time(&mut rng, user.persona);
                        session.next_event_at = sim_now + to_chrono(think);
                        (et, String::new(), session.current_page)
                    } else {
                        session.previous_page = Some(session.current_page);
                        session.current_page = navigated;
                        session.events_remaining -= 1;
                        let think = think_time(&mut rng, user.persona);
                        session.next_event_at = sim_now + to_chrono(think);
                        ("page_view", String::new(), session.current_page)
                    }
                }
            } else {
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
                    session.next_event_at = sim_now + to_chrono(think);
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
                    session.next_event_at = sim_now + to_chrono(think);
                    ("page_view", uid, session.current_page)
                }
            };

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
                event_time: sim_now.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
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

            if session.events_remaining == 0 && !session.needs_login_event {
                sessions_to_remove.push(sess_idx);
            }
        }

        // === 5. Send events to Kafka ===
        let msg_timestamp = sim_now.timestamp_millis();
        if args.backfill {
            for (tenant_id, json) in &events_to_send {
                let record = FutureRecord::to(&args.topic)
                    .key(tenant_id.as_str())
                    .payload(json.as_str())
                    .timestamp(msg_timestamp);
                let _ = producer.send(record, Duration::from_millis(500));
                total_events += 1;
                events_since_last_report += 1;
            }
        } else {
            for (tenant_id, json) in &events_to_send {
                let record = FutureRecord::to(&args.topic)
                    .key(tenant_id.as_str())
                    .payload(json.as_str())
                    .timestamp(msg_timestamp);
                if let Err((err, _)) = producer.send(record, Duration::from_secs(1)).await {
                    eprintln!("Kafka send error: {err}");
                }
                total_events += 1;
                events_since_last_report += 1;
            }
        }

        // === 6. Clean up ended sessions, schedule returns ===
        sessions_to_remove.sort_unstable();
        sessions_to_remove.dedup();
        for &sess_idx in sessions_to_remove.iter().rev() {
            let user_idx = sessions[sess_idx].user_idx;

            if sessions[sess_idx].signed_in {
                users[user_idx].is_registered = true;
            }
            users[user_idx].last_active = sim_now;

            schedule_return(&mut rng, &mut users[user_idx], sim_now);

            let last_idx = sessions.len() - 1;
            if sess_idx != last_idx {
                let moved_user_idx = sessions[last_idx].user_idx;
                if let UserState::InSession(ref mut si) = users[moved_user_idx].state {
                    *si = sess_idx;
                }
            }
            sessions.swap_remove(sess_idx);
        }

        // === 7. Status report every 10 seconds (real time) ===
        if real_now.duration_since(last_report) >= Duration::from_secs(10) {
            let real_elapsed = real_now.duration_since(last_report).as_secs_f64();
            let sim_elapsed = (sim_now - last_report_sim).num_milliseconds() as f64 / 1000.0;

            // Rate for calibration uses simulated time (correct in both modes)
            let sim_rate = if sim_elapsed > 0.0 {
                events_since_last_report as f64 / sim_elapsed
            } else {
                0.0
            };
            smoothed_rate = if smoothed_rate == 0.0 {
                sim_rate
            } else {
                0.3 * sim_rate + 0.7 * smoothed_rate
            };

            // Real throughput for display
            let real_rate = events_since_last_report as f64 / real_elapsed;

            if !sessions.is_empty() && smoothed_rate > 0.0 {
                let measured = sessions.len() as f64 / smoothed_rate;
                effective_think_secs = 0.1 * measured + 0.9 * effective_think_secs;
            }

            if args.backfill {
                let days_done = (sim_now - backfill_start_dt).num_days();
                let total_days = (backfill_end_dt - backfill_start_dt).num_days();
                let pct = if total_days > 0 {
                    (days_done as f64 / total_days as f64 * 100.0).min(100.0)
                } else {
                    100.0
                };

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

                eprint!(
                    "[BACKFILL Day {}/{} {:.0}%] sim={} | users={} sessions={} events={} sim={:.0}/s real={:.0}/s | {}",
                    days_done,
                    total_days,
                    pct,
                    sim_now.format("%Y-%m-%d %H:%M"),
                    users.len(),
                    sessions.len(),
                    total_events,
                    smoothed_rate,
                    real_rate,
                    active_by_tenant.join(" "),
                );

                let spike_info: Vec<String> = tenant_states
                    .iter()
                    .filter(|ts| ts.spike.active)
                    .map(|ts| {
                        let remaining = (ts.spike.ends_at - sim_now).num_minutes();
                        format!(
                            "{}({:.0}x, {}min left)",
                            ts.name.split('-').next().unwrap_or(&ts.name),
                            ts.spike.multiplier,
                            remaining
                        )
                    })
                    .collect();
                if !spike_info.is_empty() {
                    eprint!(" | spike: {}", spike_info.join(" "));
                }
                eprintln!();
            } else {
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
                        let remaining = (ts.spike.ends_at - sim_now).num_minutes();
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
                        let ago = sim_now - t;
                        if ago.num_hours() > 0 {
                            format!("{}h ago", ago.num_hours())
                        } else {
                            format!("{}m ago", ago.num_minutes())
                        }
                    })
                    .unwrap_or_else(|| "-".to_string());

                eprint!(
                    "[{}] users={} (registered={} returning={} churned={}) sessions={} events={} rate={:.1}/s target={:.1}/s diurnal={:.2}x drift={:.2}x oldest={} | {}",
                    sim_now.format("%H:%M:%S"),
                    total_users,
                    registered,
                    returning,
                    churned,
                    sessions.len(),
                    total_events,
                    smoothed_rate,
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
            }

            events_since_last_report = 0;
            last_report = real_now;
            last_report_sim = sim_now;
        }

        // === 8. Time advance / sleep ===
        if args.backfill {
            sim_now = sim_now + chrono::Duration::seconds(1);
        } else {
            let next_event_time = sessions.iter().map(|s| s.next_event_at).min();
            let sleep_dur = match next_event_time {
                Some(t) if t > sim_now => {
                    let delta = t - sim_now;
                    let dur = delta
                        .to_std()
                        .unwrap_or(Duration::from_secs(1))
                        .min(Duration::from_secs(1));
                    dur
                }
                Some(_) => Duration::ZERO,
                None => Duration::from_secs(1),
            };
            if !sleep_dur.is_zero() {
                tokio::time::sleep(sleep_dur).await;
            }
        }
    }

    // === Post-loop: flush and summary (backfill only) ===
    if args.backfill {
        eprintln!("Backfill generation complete. Flushing Kafka producer...");
        if let Err(e) = producer.flush(Duration::from_secs(120)) {
            eprintln!("Flush error: {e}");
        }
        let elapsed = backfill_real_start.elapsed();
        let total_days = (backfill_end_dt - backfill_start_dt).num_days();
        eprintln!(
            "Done: {} events over {} simulated days in {:.1}s ({:.0} events/s real throughput)",
            total_events,
            total_days,
            elapsed.as_secs_f64(),
            total_events as f64 / elapsed.as_secs_f64().max(0.001),
        );
    }
}
