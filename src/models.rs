use chrono::NaiveDateTime;
use rocket_db_pools::diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = crate::schema::ingests)]
pub struct NewIngest {
    pub date_started: NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::ingests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Ingest {
    pub id: i64,
    pub date_started: NaiveDateTime,
    pub date_finished: Option<NaiveDateTime>,
}