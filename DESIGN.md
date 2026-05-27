# Design System — Arcstream CDP

## Product Context
- **What this is:** A real-time Customer Data Platform reference architecture and live demo
- **Who it's for:** Engineers and architects evaluating CDP patterns, wanting to understand and implement their own
- **Space/industry:** Data infrastructure, streaming analytics, CDP
- **Project type:** Reference architecture showcase with live dashboard

## Memorable Thing
"This actually works — every piece is real, and you can understand the whole pipeline."

## Aesthetic Direction
- **Direction:** Industrial/Utilitarian — function-first, data-dense, monospace accents, muted palette
- **Decoration level:** Minimal — typography and data do the talking
- **Mood:** Serious engineering tool. Dark, quiet, precise. The architecture diagram with animated particles is the one moment of visual expression — everything else stays out of the way.

## Typography
- **Display/Hero:** Satoshi (700, 600) — geometric sans with personality, not generic
- **Body:** DM Sans (400, 500, 600) — clean and readable at small sizes, pairs well with Satoshi
- **UI/Labels:** DM Sans (500, 600) at 11-13px with letter-spacing for uppercase labels
- **Data/Tables:** Geist Mono (400, 500) with tabular-nums — aligned columns, honest numbers
- **Code:** Geist Mono
- **Loading:** Bunny Fonts CDN (`fonts.bunny.net`)
- **Scale:**
  - Hero: 28px / 700
  - Site title (header h1): 20px / 600
  - Page title: 20px / 600
  - Section heading: 18px / 600
  - Card title: 15px / 600
  - Body: 14px / 400
  - Small/labels: 13px / 500
  - Micro/badges: 11px / 600, uppercase, letter-spacing 0.5-1.5px

## Color
- **Approach:** Restrained — one warm accent, neutral grays, semantic colors only where meaningful
- **Primary accent:** `#D4944C` (warm amber) — used sparingly for active states, links, particle animations, accent borders
- **Accent hover:** `#E8A85E`
- **Accent dim:** `rgba(212, 148, 76, 0.12)` — backgrounds for badges, active time-range buttons
- **Background:** `#111115` (near-black with slight warmth)
- **Surface:** `#1c1c22` (cards, header, elevated elements)
- **Border:** `#2e2e38`
- **Text primary:** `#e8e8ec`
- **Text muted:** `#9898ae`
- **Semantic:**
  - Success/active: `#4ADE80` (green) — live indicators, active status dots, pipeline health
  - Warning/idle: `#F59E0B` (amber) — idle status, caution states
  - Error: `#EF4444` (red)
  - Green dim: `rgba(74, 222, 128, 0.12)` — backgrounds for live indicators
- **Dark mode:** This is dark-only. No light mode.
- **Chart palette** (8-slot, ordered for maximum hue separation on dark surfaces):
  1. `#D4944C` — amber (accent anchor)
  2. `#4A90D9` — cobalt
  3. `#9B6DD7` — amethyst
  4. `#36B5A0` — jade
  5. `#D96B8C` — raspberry
  6. `#E5C547` — saffron
  7. `#7E92A6` — steel
  8. `#46BA7E` — forest

## Spacing
- **Base unit:** 4px
- **Density:** Comfortable — data-dense where needed (tables, stats), breathing room elsewhere
- **Scale:** 2xs(2px) xs(4px) sm(8px) md(16px) lg(24px) xl(32px) 2xl(48px) 3xl(64px)
- **Container:** max-width 1200px, padding 24px

## Layout

### Approach
Grid-disciplined with narrative flow. The site guides visitors through a story: understand the system, then see it working live.

### Navigation
**Order:** Architecture | Profiles | Events | Analytics

Architecture first because that's why people visit. Renamed from Users/Stats to Profiles/Analytics for CDP-domain precision.

**Header:** Minimal bar — product name left, nav links + GitHub icon right. 14px padding, surface background, border-bottom.

### Route Structure
| Route | Page | Purpose |
|-------|------|---------|
| `/` | Architecture | Front door — understand the system |
| `/profiles` | Profiles | Live user profile table |
| `/events` | Events | Real-time event stream |
| `/analytics` | Analytics | Charts and aggregations |
| `/profiles/:tenant/:id` | Profile Detail | Individual user timeline |

### Architecture Page (landing at `/`)
1. **Live pulse bar** — 3 metrics (events/sec, total profiles, active sessions) in a horizontal row, each with a pulsing green dot proving the pipeline is alive
2. **Hero block** — "Reference Architecture" badge, title, description, "Click any component to explore" hint
3. **Interactive SVG diagram** — existing architecture diagram with particle animations (keep as-is, it's excellent)
4. **Install CTA** — "Run it yourself" label, curl command in monospace, copy button. Full-width card.
5. **"See live data" link** — subtle prompt leading to Profiles page
6. **Data Guarantees** — 4-column grid of guarantee cards
7. **Tech Stack** — grid of technology cards
8. **Design Decisions** — accordion/expandable list

### Live/Paused Toggle (Profiles + Events pages)
- **Placement:** In the header row, next to the live indicator dot and page title
- **States:**
  - **Live (default):** Green pulsing dot + "Live" label in green. Data updates via WebSocket tick.
  - **Paused:** Orange static dot + "Paused" label in orange (`var(--orange)`, #F59E0B). WebSocket tick is gated — no data refetch until resumed.
- **Styling:** Clickable pill button. Background `var(--green-dim)` when live, `rgba(245, 158, 11, 0.12)` when paused. 11px/600/uppercase label. `cursor: pointer`. 150ms color/background transition.
- **Behavior:** Toggling to paused freezes the current table/stream in place. Toggling back to live immediately resumes updates (next tick refetches). Toggle state is per-page, not global — pausing profiles doesn't pause events.
- **Implementation:** A `paused: RwSignal<bool>` per page. The existing `Tick` context signal is only propagated to resources when `!paused.get()`. The toggle component reads and sets this signal.

### Profiles Page (`/profiles`)
- **Header row:** Page title ("User Profiles") with live/paused toggle on the left, compact inline stats (profiles count, events count, sessions count) on the right — all in one line
- **Data table** below with columns: status dot, name+avatar, last active, events, sessions, country, device
- **Pagination** at bottom

### Events Page (`/events`)
- Page title + live/paused toggle
- Real-time event stream below

### Analytics Page (`/analytics`)
- **Header row:** "Live Analytics" title with pulsing green live dot on the left, time range selector (24h, 7d, 30d, 90d) on the right — same line
- **Live callout:** Single-line accent-bordered status bar below the header. Left border 3px in `var(--accent)` (#D4944C), pulsing green dot, 13px DM Sans text, `var(--surface)` background. Content: "Showing live results from millions of rows — events stream through Redpanda, processed by Flink, delivered by Pinot in sub-second." Reads as a status indicator, not documentation.
- **Metric cards row:** 5 cards in a horizontal row between the callout and charts. All values respond to the selected time range and animate with the odometer rolling-digit effect on change.
  - **Users** — unique active users in period (DISTINCTCOUNTHLL on canonical_id)
  - **Sessions** — total sessions in period (COUNT from sessions table)
  - **Events** — total events processed in period (COUNT from events table)
  - **Avg Duration** — mean session length in seconds, formatted as "Xm Ys" (AVG of duration_sec)
  - **Events/Session** — engagement rate, computed server-side (events / sessions, 1 decimal)
- **Metric card styling:**
  - Label: 11px / 600wt / uppercase / letter-spacing 1px / `var(--text-muted)` — DM Sans
  - Value: 24px / 500wt / `var(--text)` — Geist Mono with `font-variant-numeric: tabular-nums`
  - Subtitle: 12px / 400wt / `var(--text-muted)` — shows period context (e.g. "last 7 days")
  - Card: `var(--surface)` background, `var(--shadow-card)`, 8px border-radius, 16px padding
  - Grid: `grid-template-columns: repeat(5, 1fr)`, gap 12px. At mobile (<768px): 2 columns, last card spans full width
  - Hover: subtle border-color shift to `var(--accent-dim)`
- **Odometer animation:** Reuse existing `RollingCounter` / `RollingDigit` components from `stats_bar.rs`. When time range changes, new values roll in digit-by-digit. Duration per digit: 400ms with ease-out easing, staggered 50ms per position for a cascade effect.
- **Data source:** New `analyticsSummary(range: TimeRange!)` GraphQL query in the Query API. Returns `{ users: u64, sessions: u64, events: u64, avgDurationSec: f64, eventsPerSession: f64 }`. All computed from Pinot in a single `tokio::try_join!` of 4 queries (events/session derived server-side from events count / sessions count).
- **Chart grid:**
  - Events chart: full-width
  - Active Users + Sessions: 2-column pair
  - Avg Session Duration + Devices: 2-column pair
  - Top Pages: full-width horizontal bar chart
  - Browsers + Top Countries: 2-column pair

### Grid
- Single column at mobile (<768px)
- 2-column pairs at desktop for chart cards
- 4-column for guarantee cards (2-column at mobile)

### Border Radius
- Cards/containers: 8px
- Buttons/inputs: 6px
- Badges/pills: 100px (full round)
- Avatar: 50% (circle)
- Chart bars: 2px top corners

## Motion
- **Approach:** Minimal-functional with one expressive exception
- **Pipeline particles:** Animated SVG circles flowing along paths with glow filter — this is the signature visual element
- **Live pulse dots:** 2s ease-in-out infinite opacity+box-shadow animation on green status indicators
- **Transitions:** color/opacity changes at 150ms for hover states
- **Easing:** ease-out for enters, ease-in for exits, ease-in-out for continuous animations
- **No page transitions, no scroll animations, no loading skeletons** — the data appears instantly via SSR

## Shadows
- **Card shadow:** `0 1px 3px rgba(0,0,0,0.4), 0 0 0 1px var(--border)` — subtle depth + visible border
- **No elevation hierarchy** — flat design, borders define boundaries

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-05-26 | Architecture page as landing (/) | Visitors need to understand the system before data means anything |
| 2026-05-26 | Live pulse bar on architecture page | Proves the pipeline is alive — the "it actually works" moment |
| 2026-05-26 | Rename Users→Profiles, Stats→Analytics | CDP-domain terminology signals expertise |
| 2026-05-26 | Nav order: Architecture first | The architecture is why people visit; data pages are proof |
| 2026-05-26 | One-command install CTA on landing | Converts "interesting" into "I'll try it myself" |
| 2026-05-26 | Dark-only, no light mode | Data infrastructure tools are dark by convention; single mode reduces complexity |
| 2026-05-26 | Compact inline stats on Profiles page | Stats bar was taking vertical space without earning it; inline keeps density |
| 2026-05-26 | Time range selector in Analytics header row | Tighter hierarchy, less floating UI |
| 2026-05-27 | Live/Paused toggle on Profiles and Events pages | Users need to freeze data to explore without it shifting under their cursor |
| 2026-05-27 | Analytics page: 5 GA-style metric cards with odometer animation | Summary numbers at a glance before charts, emphasizes real-time nature |
| 2026-05-27 | Analytics blurb: accent-border callout with live dot | Status indicator treatment instead of documentation paragraph |
| 2026-05-27 | New analyticsSummary GraphQL query | Pre-aggregated totals per time range, not client-side timeseries summation |
| 2026-05-28 | "Deep Spectrum" chart palette | Old palette had 4 amber variants and 2 greens that bled together; new 8-slot palette maximizes hue separation with rich saturated tones |
