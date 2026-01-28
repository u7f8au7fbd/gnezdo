use chrono::{DateTime, Local};

pub fn format_duration(start: DateTime<Local>, end: DateTime<Local>) -> String {
    let duration = end.signed_duration_since(start);
    let total_seconds = duration.num_seconds();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let millis = duration.num_milliseconds() % 1000;
    if hours > 0 {
        format!("{}時間{}分{}秒", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}分{}秒", minutes, seconds)
    } else {
        format!("{}.{:03}秒", seconds, millis)
    }
}
