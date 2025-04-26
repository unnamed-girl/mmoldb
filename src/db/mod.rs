// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod event_types;

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

pub async fn start_ingest(start: DateTime<Utc>, conn: &mut AsyncPgConnection) -> QueryResult<()> {
    use crate::data_schema::data::ingests::dsl::*;

    NewIngest { date_started: start.naive_utc() }
        .insert_into(ingests)
        .execute(conn)
        .await
        .map(|n| {
            assert_eq!(n, 1, "New ingest should insert 1 row");
        })
}

pub async fn has_game(game_id: &str) -> QueryResult<bool> {
    Ok(false)
}
