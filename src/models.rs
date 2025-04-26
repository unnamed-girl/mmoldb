use chrono::NaiveDateTime;
use rocket_db_pools::diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::ingests)]
pub struct NewIngest {
    pub date_started: NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::ingests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Ingest {
    pub id: i64,
    pub date_started: NaiveDateTime,
    pub date_finished: Option<NaiveDateTime>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::events)]
pub struct NewEvent<'a> {
    ingest: i64,
    game_id: &'a str,
    game_event_index: i64,
    inning: i64,
    top_of_inning: bool,
    event_type: i64,
    count_balls: i64,
    count_strikes: i64,
    outs_before: i64,
    outs_after: i64,
    ends_inning: bool,
    batter_count: i64,
    batter_name: &'a str,
    pitcher_name: &'a str,
    fielder_names: Vec<&'a str>,
}