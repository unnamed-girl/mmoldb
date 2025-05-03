use chrono::NaiveDateTime;
use rocket_db_pools::diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::event_type)]
pub struct NewEventType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

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
    pub ingest: i64,
    pub game_id: &'a str,
    pub game_event_index: i32,
    pub contact_game_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_type: Option<i64>,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub ends_inning: bool,
    pub batter_count: i32,
    pub batter_name: &'a str,
    pub pitcher_name: &'a str,
    pub fielder_names: Vec<&'a str>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbEvent {
    pub id: i64,
    pub ingest: i64,
    pub game_id: String,
    pub game_event_index: i32,
    pub contact_game_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_type: Option<i64>,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub ends_inning: bool,
    pub batter_count: i32,
    pub batter_name: String,
    pub pitcher_name: String,
    // Diesel forces array columns to have nullable entries because
    // Postgres doesn't have a way to specify that they're not nullable.
    // I have a constraint that should prevent that.
    pub fielder_names: Vec<Option<String>>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::event_baserunners)]
pub struct NewBaserunner<'a> {
    pub event_id: i64,
    pub baserunner_name: &'a str,
    pub base_before: Option<i32>,
    pub base_after: Option<i32>,
    pub steal: bool,
}
