// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod event_types;
mod to_db_format;
mod taxa;

pub use crate::db::taxa::{Taxa, TaxaEventType};

use crate::ingest::EventDetail;
use crate::models::{Ingest, NewIngest};
use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, Insertable, QueryDsl, QueryResult};
use rocket_db_pools::diesel::{AsyncPgConnection, RunQueryDsl};

pub async fn latest_ingests(conn: &mut AsyncPgConnection) -> QueryResult<Vec<Ingest>> {
    use crate::data_schema::data::ingests::dsl::*;

    ingests
        .limit(10)
        .order(date_started.desc())
        .load::<Ingest>(conn)
        .await
}

pub async fn start_ingest(conn: &mut AsyncPgConnection, start: DateTime<Utc>) -> QueryResult<i64> {
    use crate::data_schema::data::ingests::dsl::*;

    NewIngest { date_started: start.naive_utc() }
        .insert_into(ingests)
        .returning(id)
        .get_result(conn)
        .await
}

pub async fn has_game(game_id: &str) -> QueryResult<bool> {
    Ok(false)
}

pub async fn insert_event<'e>(conn: &mut AsyncPgConnection, taxa: &Taxa, inning_id: i64, event: &'e EventDetail<'e>) -> QueryResult<()> {
    use crate::data_schema::data::events::dsl::*;
    use crate::data_schema::data::event_baserunners::dsl::*;

    let (new_event, new_runners) = to_db_format::event_to_row(taxa, inning_id, event);

    diesel::insert_into(events)
        .values(new_event)
        .execute(conn)
        .await
        .map(|n| assert_eq!(n, 1, "New event should insert 1 row"))?;

    let n_to_insert = new_runners.len();
    diesel::insert_into(event_baserunners)
        .values(new_runners)
        .execute(conn)
        .await
        .map(|n| {
            assert_eq!(n, n_to_insert, "Baserunners insert should insert {n_to_insert} rows")
        })?;

    Ok(())
}