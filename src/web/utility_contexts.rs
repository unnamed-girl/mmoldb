use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use rocket::serde::Serialize;

#[derive(Serialize)]
pub struct FormattedDateContext {
    relative: String,
    absolute: String,
}

impl From<&NaiveDateTime> for FormattedDateContext {
    fn from(value: &NaiveDateTime) -> Self {
        let value_utc = Utc.from_utc_datetime(value);
        let human = HumanTime::from(value_utc);

        Self {
            relative: human.to_string(),
            absolute: value_utc.to_string(),
        }
    }
}
