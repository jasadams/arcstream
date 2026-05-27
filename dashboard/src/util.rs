use chrono::{NaiveDateTime, Utc};

pub const PAGE_SIZE: u32 = 25;

pub const SVG_GITHUB: &str = r#"<svg viewBox="0 0 16 16"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>"#;
pub const SVG_MONITOR: &str = r#"<svg viewBox="0 0 256 256"><path d="M208,40H48A24,24,0,0,0,24,64V176a24,24,0,0,0,24,24H208a24,24,0,0,0,24-24V64A24,24,0,0,0,208,40Zm8,136a8,8,0,0,1-8,8H48a8,8,0,0,1-8-8V64a8,8,0,0,1,8-8H208a8,8,0,0,1,8,8Zm-48,48a8,8,0,0,1-8,8H96a8,8,0,0,1,0-16h64A8,8,0,0,1,168,224Z"/></svg>"#;
pub const SVG_TABLET: &str = r#"<svg viewBox="0 0 256 256"><path d="M192,24H64A24,24,0,0,0,40,48V208a24,24,0,0,0,24,24H192a24,24,0,0,0,24-24V48A24,24,0,0,0,192,24ZM56,72H200V184H56Zm8-32H192a8,8,0,0,1,8,8v8H56V48A8,8,0,0,1,64,40ZM192,216H64a8,8,0,0,1-8-8v-8H200v8A8,8,0,0,1,192,216Z"/></svg>"#;
pub const SVG_MOBILE: &str = r#"<svg viewBox="0 0 256 256"><path d="M176,16H80A24,24,0,0,0,56,40V216a24,24,0,0,0,24,24h96a24,24,0,0,0,24-24V40A24,24,0,0,0,176,16ZM72,64H184V192H72Zm8-32h96a8,8,0,0,1,8,8v8H72V40A8,8,0,0,1,80,32Zm96,192H80a8,8,0,0,1-8-8v-8H184v8A8,8,0,0,1,176,224Z"/></svg>"#;

pub fn is_recent(ts: &str, max_age_secs: i64) -> bool {
    let normalized = ts.replace('T', " ").trim_end_matches('Z').to_string();
    let parsed = NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S"));
    match parsed {
        Ok(dt) => (Utc::now().naive_utc() - dt).num_seconds() <= max_age_secs,
        Err(_) => false,
    }
}

pub fn parse_timestamp(ts: &str) -> Option<NaiveDateTime> {
    let normalized = ts.replace('T', " ").trim_end_matches('Z').to_string();
    NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S"))
        .ok()
}

pub fn absolute_time(ts: &str) -> String {
    match parse_timestamp(ts) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => ts.to_string(),
    }
}

pub fn relative_time(ts: &str) -> String {
    let normalized = ts.replace('T', " ").trim_end_matches('Z').to_string();
    let parsed = NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S"));
    let parsed = match parsed {
        Ok(dt) => dt,
        Err(_) => return ts.to_string(),
    };
    let now = Utc::now().naive_utc();
    let diff_secs = (now - parsed).num_seconds();
    if diff_secs < 0 {
        return "just now".to_string();
    }
    match diff_secs {
        0..=4 => "just now".into(),
        5..=59 => format!("{}s ago", diff_secs),
        60..=3599 => format!("{}m ago", diff_secs / 60),
        3600..=86399 => format!("{}h ago", diff_secs / 3600),
        86400..=2591999 => format!("{}d ago", diff_secs / 86400),
        _ => ts[..10.min(ts.len())].to_string(),
    }
}

pub fn country_flag(code: &str) -> String {
    code.to_uppercase()
        .chars()
        .map(|c| char::from_u32(0x1F1E6 + c as u32 - 'A' as u32).unwrap_or(c))
        .collect()
}

pub fn country_name(code: &str) -> &'static str {
    match code.to_uppercase().as_str() {
        "AU" => "Australia",
        "BR" => "Brazil",
        "CA" => "Canada",
        "CN" => "China",
        "DE" => "Germany",
        "ES" => "Spain",
        "FR" => "France",
        "GB" => "United Kingdom",
        "IN" => "India",
        "IT" => "Italy",
        "JP" => "Japan",
        "KR" => "South Korea",
        "MX" => "Mexico",
        "NL" => "Netherlands",
        "RU" => "Russia",
        "SE" => "Sweden",
        "US" => "United States",
        _ => "Unknown",
    }
}

pub fn truncate_id(id: &str) -> String {
    if id.len() > 8 {
        format!("{}...", &id[..8])
    } else {
        id.to_string()
    }
}

pub fn device_svg(device_type: &str) -> &'static str {
    match device_type.to_lowercase().as_str() {
        "desktop" => SVG_MONITOR,
        "tablet" => SVG_TABLET,
        "mobile" => SVG_MOBILE,
        _ => SVG_MONITOR,
    }
}
