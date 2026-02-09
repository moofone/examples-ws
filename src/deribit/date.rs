//! Small date utilities without pulling in chrono/time dependencies.
//!
//! We only need "UTC date from unix timestamp" for Deribit expiry caching.

use core::cmp::Ordering;

/// UTC calendar date.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct UtcDate {
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl Ord for UtcDate {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.year, self.month, self.day).cmp(&(other.year, other.month, other.day))
    }
}

impl PartialOrd for UtcDate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl core::fmt::Display for UtcDate {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// Convert a Unix timestamp (milliseconds) to a UTC date.
pub fn utc_date_from_unix_ms(ts_ms: i64) -> UtcDate {
    const MS_PER_DAY: i64 = 86_400_000;
    let days = ts_ms.div_euclid(MS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    UtcDate {
        year: y,
        month: m,
        day: d,
    }
}

/// Current UTC date (best-effort).
pub fn utc_today() -> Option<UtcDate> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let ms = dur.as_millis().min(i64::MAX as u128) as i64;
    Some(utc_date_from_unix_ms(ms))
}

// From Howard Hinnant's civil calendar algorithms (public domain):
// https://howardhinnant.github.io/date_algorithms.html
//
// `days` is the number of days since 1970-01-01 (Unix epoch).
fn civil_from_days(days: i64) -> (i32, u8, u8) {
    // Shift to 0000-03-01 based epoch.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096).div_euclid(365); // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2).div_euclid(153); // [0, 11]
    let d = doy - (153 * mp + 2).div_euclid(5) + 1; // [1, 31]
    let m = mp + if mp < 10 { 3 } else { -9 }; // [1, 12]
    let year = (y + (m <= 2) as i64) as i32;
    (year, m as u8, d as u8)
}
