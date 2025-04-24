use chrono::NaiveDateTime;
use rocket_db_pools::diesel::prelude::*;
use crate::schema::ingests::date_finished;
// #[derive(Insertable)]
// #[diesel(table_name = ingests)]
// struct Ingest {
//     date_created: DateTime<Utc>,
// }

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::ingests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Ingest {
    pub id: i64,
    pub date_started: NaiveDateTime,
    pub date_finished: Option<NaiveDateTime>,
}