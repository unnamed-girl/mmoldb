// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod taxa;
mod to_db_format;

pub use crate::db::taxa::{Taxa, TaxaEventType, TaxaHitType, TaxaFairBallType};

use crate::ingest::EventDetail;
use crate::models::{DbEvent, DbFielder, Ingest, NewIngest};
use chrono::{DateTime, Utc};
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

pub async fn has_game(conn: &mut AsyncPgConnection, with_id: &str) -> QueryResult<bool> {
    use diesel::dsl::*;
    use crate::data_schema::data::events::dsl::*;

    select(exists(events.filter(game_id.eq(with_id))))
        .get_result(conn)
        .await
}

pub async fn delete_events_for_game(conn: &mut AsyncPgConnection, with_id: &str) -> QueryResult<()> {
    use diesel::dsl::*;
    use crate::data_schema::data::events::dsl::*;
    
    // 

    delete(events.filter(game_id.eq(with_id)))
        .execute(conn)
        .await?;
    
    Ok(())
}

pub async fn events_for_game<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    for_game_id: &str,
) -> QueryResult<Vec<EventDetail<String>>> {
    use crate::data_schema::data::events::dsl::*;

    let db_events = events
        .filter(game_id.eq(for_game_id))
        .order(game_event_index.asc())
        .select(DbEvent::as_select())
        .load(conn)
        .await?;

    // Just get all the fielders for all the events, unassociated
    let db_fielders = DbFielder::belonging_to(&db_events)
        .select(DbFielder::as_select())
        .load(conn)
        .await?;

    // Group the fielders per book
    let db_fielders_per_event = db_fielders
        .grouped_by(&db_events);
    Ok(
        std::iter::zip(db_events, db_fielders_per_event)
            .into_iter()
            .map(|(db_event, db_fielders)| {
                to_db_format::row_to_event(taxa, db_event, db_fielders)
            })
            .collect()
    )
}

pub async fn insert_events<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    event_details: &'e [EventDetail<&'e str>],
) -> QueryResult<()> {
    use crate::data_schema::data::event_fielders::dsl as fielders_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;

    let new_events: Vec<_> = event_details.iter()
        .map(|event| {
            to_db_format::event_to_row(taxa, ingest_id, event)
        })
        .collect();

    let event_ids: Vec<i64> = diesel::insert_into(events_dsl::events)
        .values(new_events)
        .returning(events_dsl::id)
        .get_results(conn)
        .await?;

    assert_eq!(
        event_ids.len(), 
        event_details.len(), 
        "Events insert should insert {} rows", 
        event_details.len(),
    );

    let new_fielders: Vec<_> = event_details.iter()
        .zip(&event_ids)
        .flat_map(|(event, &event_id)| {
            to_db_format::event_to_fielders(taxa, event_id, event)
        })
        .collect();
    let n_fielders_to_insert = new_fielders.len();

    let n_fielders_inserted = diesel::insert_into(fielders_dsl::event_fielders)
        .values(new_fielders)
        .execute(conn)
        .await?;

    assert_eq!(
        n_fielders_inserted,
        n_fielders_to_insert,
        "Fielders insert should insert {n_fielders_to_insert} rows",
    );
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
