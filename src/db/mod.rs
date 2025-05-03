// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod taxa;
mod to_db_format;

pub use crate::db::taxa::{Taxa, TaxaEventType, TaxaHitType};

use crate::ingest::EventDetail;
use crate::models::{DbEvent, Ingest, NewIngest};
use chrono::{DateTime, Utc};
use rocket::futures::StreamExt;
use rocket::http::hyper::body::HttpBody;
use rocket_db_pools::diesel::AsyncPgConnection;
use rocket_db_pools::diesel::prelude::*;

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

    NewIngest {
        date_started: start.naive_utc(),
    }
    .insert_into(ingests)
    .returning(id)
    .get_result(conn)
    .await
}

pub async fn has_game(game_id: &str) -> QueryResult<bool> {
    Ok(false)
}

pub async fn events_for_game<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    for_game_id: &str,
) -> QueryResult<Vec<EventDetail<String>>> {
    use crate::data_schema::data::event_baserunners::dsl::*;
    use crate::data_schema::data::events::dsl::*;

    Ok(
        events
            .filter(game_id.eq(for_game_id))
            .order(game_event_index.asc())
            .select(DbEvent::as_select())
            .load(conn)
            .await?
            .into_iter()
            .map(|db_event| to_db_format::row_to_event(taxa, db_event))
            .collect()
    )
}

pub async fn insert_events<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    event_details: &'e [EventDetail<&'e str>],
) -> QueryResult<()> {
    use crate::data_schema::data::event_baserunners::dsl::*;
    use crate::data_schema::data::events::dsl::*;

    let new_events: Vec<_> = event_details.iter()
        .map(|event| {
            // TODO Handle runners
            let (new_event, new_runners) = to_db_format::event_to_row(taxa, ingest_id, event);
            new_event
        })
        .collect();
    let events_to_insert = new_events.len();

    diesel::insert_into(events)
        .values(new_events)
        .execute(conn)
        .await
        .map(|n| assert_eq!(n, events_to_insert, "Events insert should insert {events_to_insert} rows"))?;

    // let n_to_insert = new_runners.len();
    // diesel::insert_into(event_baserunners)
    //     .values(new_runners)
    //     .execute(conn)
    //     .await
    //     .map(|n| {
    //         assert_eq!(
    //             n, n_to_insert,
    //             "Baserunners insert should insert {n_to_insert} rows"
    //         )
    //     })?;

    Ok(())
}
