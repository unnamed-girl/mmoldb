use chrono::NaiveDateTime;
use rocket_db_pools::diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::event_type)]
pub struct NewEventType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::hit_type)]
pub struct NewHitType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::position)]
pub struct NewPosition<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::fair_ball_type)]
pub struct NewFairBallType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::base)]
pub struct NewBase<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::base_description_format)]
pub struct NewBaseDescriptionFormat<'a> {
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
    pub fair_ball_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_type: Option<i64>,
    pub fair_ball_type: Option<i64>,
    pub fair_ball_direction: Option<i64>,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub batter_count: i32,
    pub batter_name: &'a str,
    pub pitcher_name: &'a str,
}
#[derive(Queryable, Selectable, Identifiable)]
#[diesel(table_name = crate::data_schema::data::events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbEvent {
    pub id: i64,
    pub ingest: i64,
    pub game_id: String,
    pub game_event_index: i32,
    pub fair_ball_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_type: Option<i64>,
    pub fair_ball_type: Option<i64>,
    pub fair_ball_direction: Option<i64>,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub batter_count: i32,
    pub batter_name: String,
    pub pitcher_name: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::event_baserunners)]
pub struct NewBaserunner<'a> {
    pub event_id: i64,
    pub baserunner_name: &'a str,
    pub base_before: Option<i64>,
    pub base_after: i64,
    pub is_out: bool,
    pub base_description_format: Option<i64>,
    pub steal: bool,
}

#[derive(Queryable, Selectable, Identifiable, Associations)]
#[diesel(belongs_to(DbEvent, foreign_key = event_id))]
#[diesel(table_name = crate::data_schema::data::event_baserunners)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbRunner {
    pub id: i64,
    pub event_id: i64,
    pub baserunner_name: String,
    pub base_before: Option<i64>,
    pub base_after: i64,
    pub is_out: bool,
    pub base_description_format: Option<i64>,
    pub steal: bool,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::event_fielders)]
pub struct NewFielder<'a> {
    pub event_id: i64,
    pub fielder_name: &'a str,
    pub fielder_position: i64,
    pub play_order: i32,
}

#[derive(Queryable, Selectable, Identifiable, Associations)]
#[diesel(belongs_to(DbEvent, foreign_key = event_id))]
#[diesel(table_name = crate::data_schema::data::event_fielders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbFielder {
    pub id: i64,
    pub event_id: i64,
    pub fielder_name: String,
    pub fielder_position: i64,
    pub play_order: i32,
}
