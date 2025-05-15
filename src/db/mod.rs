// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod taxa;
mod to_db_format;

pub use crate::db::taxa::{
    Taxa, TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat, TaxaEventType,
    TaxaFairBallType, TaxaFieldingErrorType, TaxaHitType, TaxaPosition,
};

use crate::ingest::EventDetail;
use crate::models::{DbEvent, DbFielder, DbGame, DbRunner, Ingest, NewGame, NewIngest};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rocket_db_pools::{diesel::AsyncPgConnection, diesel::prelude::*};

pub async fn ingest_count(conn: &mut AsyncPgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::ingests::dsl::*;

    ingests.count().get_result(conn).await
}

pub async fn latest_ingests(conn: &mut AsyncPgConnection) -> QueryResult<Vec<(Ingest, i64)>> {
    use diesel::sql_types::*;
    #[derive(QueryableByName)]
    struct IngestWithGameCount {
        #[diesel(sql_type = Int8)]
        pub id: i64,
        #[diesel(sql_type = Timestamp)]
        pub date_started: NaiveDateTime,
        #[diesel(sql_type = Nullable<Timestamp>)]
        pub date_finished: Option<NaiveDateTime>,
        #[diesel(sql_type = Int8)]
        pub num_games: i64,
    }

    diesel::sql_query(
        "
        select i.*, count(g.mmolb_game_id) as num_games
        from data.ingests i
             left join data.games g on g.ingest = i.id
        group by i.id, i.date_started
        order by i.date_started desc
        limit 10
    ",
    )
    .load::<IngestWithGameCount>(conn)
    .await
    .map(|ok| {
        ok.into_iter()
            .map(
                |IngestWithGameCount {
                     id,
                     date_started,
                     date_finished,
                     num_games,
                 }| {
                    (
                        Ingest {
                            id,
                            date_started,
                            date_finished,
                        },
                        num_games,
                    )
                },
            )
            .collect()
    })
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

pub async fn mark_ingest_finished(
    conn: &mut AsyncPgConnection,
    ingest_id: i64,
    end: DateTime<Utc>,
) -> QueryResult<()> {
    use crate::data_schema::data::ingests::dsl::*;

    diesel::update(ingests.filter(id.eq(ingest_id)))
        .set(date_finished.eq(end.naive_utc()))
        .execute(conn)
        .await
        .map(|_| ())
}

pub async fn ingest_with_games(
    conn: &mut AsyncPgConnection,
    for_ingest_id: i64,
) -> QueryResult<(Ingest, Vec<DbGame>)> {
    use crate::data_schema::data::games::dsl as game_dsl;
    use crate::data_schema::data::ingests::dsl as ingest_dsl;

    let ingest = ingest_dsl::ingests
        .filter(ingest_dsl::id.eq(for_ingest_id))
        .get_result::<Ingest>(conn)
        .await?;
    let games = DbGame::belonging_to(&ingest)
        .order_by((game_dsl::season.asc(), game_dsl::day.asc()))
        .get_results::<DbGame>(conn)
        .await?;

    Ok((ingest, games))
}

pub async fn has_game(conn: &mut AsyncPgConnection, with_id: &str) -> QueryResult<bool> {
    use crate::data_schema::data::games::dsl::*;
    use diesel::dsl::*;

    select(exists(games.filter(mmolb_game_id.eq(with_id))))
        .get_result(conn)
        .await
}

pub async fn delete_game(conn: &mut AsyncPgConnection, with_id: &str) -> QueryResult<usize> {
    use crate::data_schema::data::games::dsl::*;
    use diesel::dsl::*;

    delete(games.filter(mmolb_game_id.eq(with_id)))
        .execute(conn)
        .await
}

pub async fn events_for_game<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    for_game_id: &str,
) -> QueryResult<Vec<EventDetail<String>>> {
    use crate::data_schema::data::event_baserunners::dsl as runner_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielder_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    let game_id = games_dsl::games
        .filter(games_dsl::mmolb_game_id.eq(for_game_id))
        .select(games_dsl::id)
        .get_result::<i64>(conn)
        .await?;

    let db_events = events_dsl::events
        .filter(events_dsl::game_id.eq(game_id))
        .order(events_dsl::game_event_index.asc())
        .select(DbEvent::as_select())
        .load(conn)
        .await?;

    let db_runners = DbRunner::belonging_to(&db_events)
        .order((
            runner_dsl::event_id,
            runner_dsl::base_before.desc().nulls_last(),
        ))
        .select(DbRunner::as_select())
        .load(conn)
        .await?
        .grouped_by(&db_events);

    let db_fielders = DbFielder::belonging_to(&db_events)
        .order((fielder_dsl::event_id, fielder_dsl::play_order))
        .select(DbFielder::as_select())
        .load(conn)
        .await?
        .grouped_by(&db_events);

    // This complicated-looking statement just zips all the iterators
    // together and passes the corresponding elements to row_to_event
    Ok(db_events
        .into_iter()
        .zip(db_runners)
        .zip(db_fielders)
        .map(|((db_event, db_runners), db_fielders)| {
            to_db_format::row_to_event(
                taxa,
                db_event,
                db_runners,
                db_fielders,
            )
        })
        .collect())
}

pub async fn insert_game<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    mmolb_game_id: &str,
    game_data: &mmolb_parsing::game::Game,
    event_details: &'e [EventDetail<&'e str>],
) -> QueryResult<()> {
    use crate::data_schema::data::event_baserunners::dsl as baserunners_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielders_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    let game_id = diesel::insert_into(games_dsl::games)
        .values(&NewGame {
            ingest: ingest_id,
            mmolb_game_id,
            season: game_data.season as i32,
            day: game_data.day as i32,
            away_team_emoji: &game_data.away_team_emoji,
            away_team_name: &game_data.away_team_name,
            home_team_emoji: &game_data.home_team_emoji,
            home_team_name: &game_data.home_team_name,
        })
        .returning(games_dsl::id)
        .get_result::<i64>(conn)
        .await?;

    let new_events: Vec<_> = event_details
        .iter()
        .map(|event| to_db_format::event_to_row(taxa, game_id, event))
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

    let new_advances: Vec<_> = event_details
        .iter()
        .zip(&event_ids)
        .flat_map(|(event, &event_id)| to_db_format::event_to_baserunners(taxa, event_id, event))
        .collect();
    let n_advances_to_insert = new_advances.len();

    let n_advances_inserted = diesel::insert_into(baserunners_dsl::event_baserunners)
        .values(new_advances)
        .execute(conn)
        .await?;

    assert_eq!(
        n_advances_inserted, n_advances_to_insert,
        "Advances insert should insert {n_advances_to_insert} rows",
    );

    let new_fielders: Vec<_> = event_details
        .iter()
        .zip(&event_ids)
        .flat_map(|(event, &event_id)| to_db_format::event_to_fielders(taxa, event_id, event))
        .collect();
    let n_fielders_to_insert = new_fielders.len();

    let n_fielders_inserted = diesel::insert_into(fielders_dsl::event_fielders)
        .values(new_fielders)
        .execute(conn)
        .await?;

    assert_eq!(
        n_fielders_inserted, n_fielders_to_insert,
        "Fielders insert should insert {n_fielders_to_insert} rows",
    );

    Ok(())
}
