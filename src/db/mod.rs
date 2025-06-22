// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod taxa;
mod to_db_format;
mod taxa_macro;

use diesel::sql_types::*;

pub use crate::db::taxa::{
    Taxa, TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat, TaxaEventType,
    TaxaFairBallType, TaxaFieldingErrorType, TaxaHitType, TaxaPitchType, TaxaPosition,
};
use std::iter;
use crate::ingest::{EventDetail, IngestLog};
use crate::models::{DbEvent, DbEventIngestLog, DbFielder, DbGame, DbRawEvent, DbRunner, Ingest, NewEventIngestLog, NewGame, NewGameIngestTimings, NewIngest, NewRawEvent};
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::dsl::count_distinct;
use diesel::query_dsl::methods::OrderDsl;
use itertools::{Itertools, PeekingNext};
use log::info;
use rocket_sync_db_pools::{diesel::PgConnection, diesel::prelude::*};
pub use to_db_format::RowToEventError;

pub fn ingest_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::info_schema::info::ingests::dsl;

    dsl::ingests.count().get_result(conn)
}

pub fn is_finished(conn: &mut PgConnection, ids: Vec<&str>) -> QueryResult<Vec<bool>> {
    use crate::data_schema::data::games::dsl;
    
    dsl::games
        .filter(dsl::mmolb_game_id.eq_any(ids))
        .select(dsl::is_finished)
        .get_results(conn)
}

pub fn game_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::games::dsl::*;

    games.count().get_result(conn)
}

pub fn game_with_issues_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::info_schema::info::event_ingest_log::dsl;

    dsl::event_ingest_log
        .filter(dsl::log_level.lt(3)) // Selects warnings and higher
        .select(count_distinct(dsl::game_id))
        .get_result(conn)
}

pub fn latest_ingest_start_time(
    conn: &mut PgConnection,
) -> QueryResult<Option<NaiveDateTime>> {
    use crate::info_schema::info::ingests::dsl::*;

    ingests
        .select(started_at)
        .order_by(started_at.desc())
        .first(conn)
        .optional()
}

pub fn last_completed_page(
    conn: &mut PgConnection,
) -> QueryResult<Option<String>> {
    use crate::info_schema::info::ingests::dsl::*;

    ingests
        .filter(last_completed_page.is_not_null())
        .select(last_completed_page)
        .order_by(started_at.desc())
        .first(conn)
        .optional()
        .map(Option::flatten)
}

#[derive(QueryableByName)]
pub struct IngestWithGameCount {
    #[diesel(sql_type = Int8)]
    pub id: i64,
    #[diesel(sql_type = Timestamp)]
    pub started_at: NaiveDateTime,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub finished_at: Option<NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub aborted_at: Option<NaiveDateTime>,
    #[diesel(sql_type = Int8)]
    pub num_games: i64,
}

pub fn latest_ingests(conn: &mut PgConnection) -> QueryResult<Vec<IngestWithGameCount>> {
    diesel::sql_query(
        "
        select i.id, i.started_at, i.finished_at, i.aborted_at, count(g.mmolb_game_id) as num_games
        from info.ingests i
             left join data.games g on g.ingest = i.id
        group by i.id, i.started_at
        order by i.started_at desc
        limit 25
    ",
    )
    .load::<IngestWithGameCount>(conn)
}

pub fn start_ingest(conn: &mut PgConnection, at: DateTime<Utc>) -> QueryResult<i64> {
    use crate::info_schema::info::ingests::dsl::*;

    NewIngest {
        started_at: at.naive_utc(),
    }
    .insert_into(ingests)
    .returning(id)
    .get_result(conn) 
}

pub fn mark_ingest_finished(
    conn: &mut PgConnection,
    ingest_id: i64,
    at: DateTime<Utc>,
    last_completed_page: Option<&str>,
) -> QueryResult<()> {
    use crate::info_schema::info::ingests::dsl;

    diesel::update(dsl::ingests.filter(dsl::id.eq(ingest_id)))
        .set((
            dsl::finished_at.eq(at.naive_utc()),
            dsl::last_completed_page.eq(last_completed_page),
         ))
        .execute(conn)
        .map(|_| ())
}

// It is assumed that an aborted ingest won't have a later 
// latest_completed_season than the previous ingest. That is not 
// necessarily true, but it's inconvenient to refactor the code to
// support it.
pub fn mark_ingest_aborted(
    conn: &mut PgConnection,
    ingest_id: i64,
    at: DateTime<Utc>,
) -> QueryResult<()> {
    use crate::info_schema::info::ingests::dsl::*;

    diesel::update(ingests.filter(id.eq(ingest_id)))
        .set(aborted_at.eq(at.naive_utc()))
        .execute(conn)
        .map(|_| ())
}

macro_rules! log_only_assert {
    ($e: expr, $($msg:tt)*) => {
        if !$e {
            log::error!($($msg)*)
        }
    };
}
 fn add_log_levels_to_games(
    conn: &mut PgConnection,
    games: Vec<DbGame>,
) -> QueryResult<Vec<(DbGame, i64, i64, i64)>> {
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_event_dsl;

    let game_ids = games.iter().map(|g| g.id).collect::<Vec<i64>>();

    fn count_log_level(
        conn: &mut PgConnection,
        game_ids: &Vec<i64>,
        level: i32,
    ) -> QueryResult<Vec<(i64, i64)>> {
        raw_event_dsl::raw_events
            .filter(raw_event_dsl::game_id.eq_any(game_ids))
            .left_join(event_ingest_log_dsl::event_ingest_log.on(event_ingest_log_dsl::game_id.eq(raw_event_dsl::game_id).and(event_ingest_log_dsl::game_event_index.eq(raw_event_dsl::game_event_index))))
            .filter(event_ingest_log_dsl::log_level.eq(level))
            .group_by(raw_event_dsl::game_id)
            .select((raw_event_dsl::game_id, diesel::dsl::count_star()))
            .order_by(raw_event_dsl::game_id)
            .get_results::<(i64, i64)>(conn)
    }

    let mut num_warnings = count_log_level(conn, &game_ids, 2)?
        .into_iter()
        .peekable();
    let mut num_errors = count_log_level(conn, &game_ids, 1)?
        .into_iter()
        .peekable();
    let mut num_critical = count_log_level(conn, &game_ids, 0)?
        .into_iter()
        .peekable();

    let games = games
        .into_iter()
        .map(|game| {
            let warnings = num_warnings
                .peeking_next(|(id, _)| *id == game.id)
                .map_or(0, |(_, n)| n);
            let errors = num_errors
                .peeking_next(|(id, _)| *id == game.id)
                .map_or(0, |(_, n)| n);
            let critical = num_critical
                .peeking_next(|(id, _)| *id == game.id)
                .map_or(0, |(_, n)| n);
            (game, warnings, errors, critical)
        })
        .collect();

    log_only_assert!(
        num_warnings.next().is_none(),
        "db::ingest_with_games failed to match at least one warnings count with its event",
    );
    log_only_assert!(
        num_errors.next().is_none(),
        "db::ingest_with_games failed to match at least one errors count with its event",
    );
    log_only_assert!(
        num_critical.next().is_none(),
        "db::ingest_with_games failed to match at least one critical count with its event",
    );

    Ok(games)
}

pub fn ingest_with_games(
    conn: &mut PgConnection,
    for_ingest_id: i64,
) -> QueryResult<(Ingest, Vec<(DbGame, i64, i64, i64)>)> {
    use crate::data_schema::data::games::dsl as game_dsl;
    use crate::info_schema::info::ingests::dsl as ingest_dsl;

    let ingest = ingest_dsl::ingests
        .filter(ingest_dsl::id.eq(for_ingest_id))
        .get_result::<Ingest>(conn)?;

    let games = DbGame::belonging_to(&ingest)
        // The .select is not necessary but yields better compile errors
        .select(DbGame::as_select())
        .order_by(game_dsl::id)
        .get_results::<DbGame>(conn)?;

    let games = add_log_levels_to_games(conn, games)?;

    Ok((ingest, games))
}

pub fn has_game(conn: &mut PgConnection, with_id: &str) -> QueryResult<bool> {
    use crate::data_schema::data::games::dsl::*;
    use diesel::dsl::*;

    select(exists(games.filter(mmolb_game_id.eq(with_id))))
        .get_result(conn)
}

pub async fn delete_game(conn: &mut PgConnection, with_id: &str) -> QueryResult<usize> {
    use crate::data_schema::data::games::dsl::*;
    use diesel::dsl::*;

    delete(games.filter(mmolb_game_id.eq(with_id)))
        .execute(conn)
}

pub fn all_games(conn: &mut PgConnection) -> QueryResult<Vec<(DbGame, i64, i64, i64)>> {
    use crate::data_schema::data::games::dsl as games_dsl;

    let games = games_dsl::games
        .order_by(games_dsl::id)
        .get_results::<DbGame>(conn)?;

    add_log_levels_to_games(conn, games)
}

pub struct EventsForGameTimings {
    pub get_game_id_duration: f64,
    pub get_events_duration: f64,
    pub group_events_duration: f64,
    pub get_runners_duration: f64,
    pub group_runners_duration: f64,
    pub get_fielders_duration: f64,
    pub group_fielders_duration: f64,
    pub post_process_duration: f64,
}

pub fn events_for_games(
    conn: &mut PgConnection,
    taxa: &Taxa,
    for_game_ids: Vec<&str>,
) -> QueryResult<(
    Vec<Result<EventDetail<String>, RowToEventError>>,
    EventsForGameTimings,
)> {
    use crate::data_schema::data::event_baserunners::dsl as runner_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielder_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    let get_game_ids_start = Utc::now();
    let game_ids = games_dsl::games
        .filter(games_dsl::mmolb_game_id.eq_any(for_game_ids))
        .select(games_dsl::id)
        .get_results::<i64>(conn)?;
    let get_game_ids_duration = (Utc::now() - get_game_ids_start).as_seconds_f64();

    let get_events_start = Utc::now();
    let mut db_events_iter = events_dsl::events
        .filter(events_dsl::game_id.eq_any(game_ids))
        .order_by(events_dsl::game_event_index.asc())
        .select(DbEvent::as_select())
        .load(conn)?
        .into_iter()
        .peekable();
    let get_events_duration = (Utc::now() - get_events_start).as_seconds_f64();
    
    let group_events_start = Utc::now();
    let db_events = game_ids.iter()
        .map(|id| {
            let mut game_events = Vec::new();
            while let Some(event) = db_events_iter.next_if(|e| e.game_id == id) {
                game_events.push(event);
            }
            game_events
        })
        .collect_vec();
    let group_events_duration = (Utc::now() - group_events_start).as_seconds_f64();

    let get_runners_start = Utc::now();
    let db_runners = DbRunner::belonging_to(&db_events)
        .order_by((
            runner_dsl::event_id,
            runner_dsl::base_before.desc().nulls_last(),
        ))
        .select(DbRunner::as_select())
        .load(conn)?;
    let get_runners_duration = (Utc::now() - get_runners_start).as_seconds_f64();

    let group_runners_start = Utc::now();
    let db_runners = db_runners.grouped_by(&db_events);
    let group_runners_duration = (Utc::now() - group_runners_start).as_seconds_f64();

    let get_fielders_start = Utc::now();
    let db_fielders = DbFielder::belonging_to(&db_events)
        .order_by((fielder_dsl::event_id, fielder_dsl::play_order))
        .select(DbFielder::as_select())
        .load(conn)?;
    let get_fielders_duration = (Utc::now() - get_fielders_start).as_seconds_f64();

    let group_fielders_start = Utc::now();
    let db_fielders = db_fielders.grouped_by(&db_events);
    let group_fielders_duration = (Utc::now() - group_fielders_start).as_seconds_f64();

    // This complicated-looking statement just zips all the iterators
    // together and passes the corresponding elements to row_to_event
    let post_process_start = Utc::now();
    let result = db_events
        .into_iter()
        .zip(db_runners)
        .zip(db_fielders)
        .map(|((db_event, db_runners), db_fielders)| {
            to_db_format::row_to_event(taxa, db_event, db_runners, db_fielders)
        })
        .collect();
    let post_process_duration = (Utc::now() - post_process_start).as_seconds_f64();

    Ok((
        result,
        EventsForGameTimings {
            get_game_id_duration: get_game_ids_duration,
            get_events_duration,
            group_events_duration,
            get_runners_duration,
            group_runners_duration,
            get_fielders_duration,
            group_fielders_duration,
            post_process_duration,
        },
    ))
}

pub(crate) struct CompletedGameForDb<'g> {
    pub id: &'g str,
    pub raw_game: &'g mmolb_parsing::Game,
    pub events: Vec<EventDetail<&'g str>>,
    pub logs: Vec<Vec<IngestLog>>,
}

pub(crate) enum GameForDb<'g> {
    Incomplete {
        game_id: &'g str,
        raw_game: &'g mmolb_parsing::Game,
    },
    Completed {
        description: String,
        game: CompletedGameForDb<'g>,
    }
}

pub fn insert_games(
    conn: &mut PgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    games: Vec<GameForDb>,
) -> QueryResult<Vec<i64>> {
    conn.transaction(|conn| {
        // TODO: When a batch fails, split it in half (recursively if necessary) and try again 
        //   until it succeeds or the batch size is 1. Then return some indication of which inserts 
        //   failed.
        insert_games_internal(conn, taxa, ingest_id, games)
    })
}

fn insert_games_internal<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    games: Vec<GameForDb>,
) -> QueryResult<Vec<i64>> {
    use crate::data_schema::data::event_baserunners::dsl as baserunners_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielders_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;
    
    // First delete all games. If particular debug settings are turned on this may happen for every
    // game, but even in release mode we may need to delete partial games and replace them with
    // full games.
    let game_mmolb_ids = games.iter()
        .map(|g| match g {
            GameForDb::Incomplete { game_id, .. } => { *game_id }
            GameForDb::Completed { game, .. } => { game.id }
        })
        .collect_vec();

    info!("Pruning any existing records for {} games", game_mmolb_ids.len());
    let n_deleted_games = diesel::delete(games_dsl::games)
        .filter(games_dsl::mmolb_game_id.eq_any(game_mmolb_ids))
        .execute(conn)?;
    info!("Deleted {} games that will be replaced", n_deleted_games);

    let new_games = games.iter()
        .map(|game| {
            match game {
                GameForDb::Incomplete { game_id, raw_game } => {
                    NewGame {
                        ingest: ingest_id,
                        mmolb_game_id: game_id,
                        season: raw_game.season as i32,
                        day: raw_game.day as i32,
                        away_team_emoji: &raw_game.away_team_emoji,
                        away_team_name: &raw_game.away_team_name,
                        away_team_id: &raw_game.away_team_id,
                        home_team_emoji: &raw_game.home_team_emoji,
                        home_team_name: &raw_game.home_team_name,
                        home_team_id: &raw_game.home_team_id,
                        is_finished: false,
                    }
                }
                GameForDb::Completed { game, .. } => {
                    NewGame {
                        ingest: ingest_id,
                        mmolb_game_id: &game.id,
                        season: game.raw_game.season as i32,
                        day: game.raw_game.day as i32,
                        away_team_emoji: &game.raw_game.away_team_emoji,
                        away_team_name: &game.raw_game.away_team_name,
                        away_team_id: &game.raw_game.away_team_id,
                        home_team_emoji: &game.raw_game.home_team_emoji,
                        home_team_name: &game.raw_game.home_team_name,
                        home_team_id: &game.raw_game.home_team_id,
                        is_finished: true,
                    }
                }
            }
        })
        .collect_vec();

    let n_games_to_insert = new_games.len();
    info!("Trying to insert {n_games_to_insert} games");
    let game_ids = diesel::insert_into(games_dsl::games)
        .values(new_games)
        .returning(games_dsl::id)
        .get_results::<i64>(conn)?;
    info!("Inserted {} games", game_ids.len());

    log_only_assert!(
        n_games_to_insert == game_ids.len(),
        "Games insert should have inserted {} rows, but it inserted {}",
        n_games_to_insert, game_ids.len(),
    );

    // From now on, we don't need unfinished games
    let games = iter::zip(&game_ids, &games)
        .flat_map(|(game_id, game)| match game {
            GameForDb::Incomplete { .. } => { None }
            GameForDb::Completed { game, .. } => { Some((game_id, game)) }
        })
        .collect_vec();

    let new_raw_events = games.iter()
        .flat_map(|(game_id, game)| {
            game.raw_game.event_log.iter()
                .enumerate()
                .map(|(index, raw_event)| {
                    NewRawEvent {
                        game_id: **game_id,
                        game_event_index: index as i32,
                        event_text: &raw_event.message,
                    }
                })
        })
        .collect::<Vec<_>>();

    let n_raw_events_to_insert = new_raw_events.len();
    info!("Trying to insert {n_raw_events_to_insert} raw events");
    let n_raw_events_inserted = diesel::copy_from(raw_events_dsl::raw_events)
        .from_insertable(&new_raw_events)
        .execute(conn)?;

    log_only_assert!(
        n_raw_events_to_insert == n_raw_events_inserted,
        "Raw events insert should have inserted {} rows, but it inserted {}",
        n_raw_events_to_insert, n_raw_events_inserted,
    );

    let new_logs = games.iter()
        .flat_map(|(game_id, game)| {
            game.logs.iter()
                .enumerate()
                .flat_map(move |(game_event_index, logs)| {
                    logs.iter()
                        .enumerate()
                        .map(move |(log_index, log)| {
                            assert_eq!(game_event_index, log.game_event_index);
                            NewEventIngestLog {
                                game_id: **game_id,
                                game_event_index: game_event_index as i32,
                                log_index: log_index as i32,
                                log_level: log.log_level,
                                log_text: &log.log_text,
                            }
                        })
                })
        })
        .collect_vec();

    let n_logs_to_insert = new_logs.len();
    info!("Trying to insert {n_logs_to_insert} logs");
    let n_logs_inserted = diesel::copy_from(event_ingest_log_dsl::event_ingest_log)
        .from_insertable(&new_logs)
        .execute(conn)?;

    log_only_assert!(
        n_logs_to_insert == n_logs_inserted,
        "Event ingest logs insert should have inserted {} rows, but it inserted {}",
        n_logs_to_insert, n_logs_inserted,
    );

    let new_events: Vec<_> = games.iter()
        .flat_map(|(game_id, game)| {
            game.events.iter()
                .map(|event| to_db_format::event_to_row(taxa, **game_id, event))
        })
        .collect();

    let n_events_to_insert = new_events.len();
    info!("Trying to insert {n_events_to_insert} events");
    let n_events_inserted = diesel::copy_from(events_dsl::events)
        .from_insertable(&new_events)
        .execute(conn)?;

    log_only_assert!(
        n_events_to_insert == n_events_inserted,
        "Events insert should have inserted {} rows, but it inserted {}",
        n_events_to_insert, n_events_inserted,
    );

    // Postgres' copy doesn't support returning ids, but we need them, so we query them from scratch
    let event_ids = events_dsl::events
        .filter(events_dsl::game_id.eq_any(&game_ids))
        .select((events_dsl::game_id, events_dsl::id))
        .order_by(events_dsl::game_id)
        .get_results::<(i64, i64)>(conn)?;

    let event_ids_by_game = event_ids.into_iter()
        .chunk_by(|(game_id, _)| *game_id)
        .into_iter()
        .map(|(game_id, group)| (
            game_id, 
            group.map(|(_, event_id)| event_id).collect_vec(),
        ))
        .collect_vec();

    let new_baserunners = iter::zip(&event_ids_by_game, &games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, *game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events)
                .flat_map(|(event_id, event)| {
                    // Within this closure we're acting on a single event
                    to_db_format::event_to_baserunners(taxa, *event_id, event)
                })
        })
        .collect_vec();

    let n_baserunners_to_insert = new_baserunners.len();
    info!("Trying to insert {n_baserunners_to_insert} baserunners");
    let n_baserunners_inserted = diesel::copy_from(baserunners_dsl::event_baserunners)
        .from_insertable(&new_baserunners)
        .execute(conn)?;

    log_only_assert!(
        n_baserunners_to_insert == n_baserunners_inserted,
        "Event baserunners insert should have inserted {} rows, but it inserted {}",
        n_baserunners_to_insert, n_baserunners_inserted,
    );

    let new_fielders = iter::zip(&event_ids_by_game, &games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, *game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events)
                .flat_map(|(event_id, event)| {
                    // Within this closure we're acting on a single event
                    to_db_format::event_to_fielders(taxa, *event_id, event)
                })
        })
        .collect_vec();

    let n_fielders_to_insert = new_fielders.len();
    info!("Trying to insert {n_fielders_to_insert} fielders");
    let n_fielders_inserted = diesel::copy_from(fielders_dsl::event_fielders)
        .from_insertable(&new_fielders)
        .execute(conn)?;

    log_only_assert!(
        n_fielders_to_insert == n_fielders_inserted,
        "Event fielders insert should have inserted {} rows, but it inserted {}",
        n_fielders_to_insert, n_fielders_inserted,
    );

    Ok(game_ids)
}

pub fn insert_additional_ingest_logs(
    conn: &mut PgConnection,
    game_id: i64,
    extra_ingest_logs: impl IntoIterator<Item = (usize, IngestLog)>,
) -> QueryResult<()> {
    // use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    // use crate::info_schema::info::raw_events::dsl as raw_events_dsl;
    //
    // // index is game_event_index, value is raw_event id
    // let raw_event_ids: Vec<i64> = raw_events_dsl::raw_events
    //     .select(raw_events_dsl::id)
    //     .filter(raw_events_dsl::game_id.eq(game_id))
    //     .order_by(raw_events_dsl::game_event_index.asc())
    //     .get_results(conn)
    //     .await?;
    //
    // // Maps raw_event_id to its highest log_order
    // let mut highest_log_order_for_event: HashMap<_, _> = event_ingest_log_dsl::event_ingest_log
    //     .group_by(event_ingest_log_dsl::raw_event_id)
    //     .select((
    //         event_ingest_log_dsl::raw_event_id,
    //         diesel::dsl::max(event_ingest_log_dsl::log_order),
    //     ))
    //     .filter(event_ingest_log_dsl::raw_event_id.eq_any(&raw_event_ids))
    //     .order_by(event_ingest_log_dsl::raw_event_id.asc())
    //     .get_results::<(i64, Option<i32>)>(conn)
    //     .await?
    //     .into_iter()
    //     .filter_map(|(id, num)| num.map(|n| (id, n)))
    //     .collect();
    //
    // let new_logs: Vec<_> = extra_ingest_logs
    //     .into_iter()
    //     .map(|(game_event_index, ingest_log)| {
    //         let raw_event_id = raw_event_ids[game_event_index];
    //         let n = highest_log_order_for_event.entry(raw_event_id).or_default();
    //         *n += 1;
    //
    //         NewEventIngestLog {
    //             raw_event_id,
    //             log_order: *n,
    //             log_level: ingest_log.log_level,
    //             log_text: ingest_log.log_text,
    //         }
    //     })
    //     .collect();
    //
    // diesel::insert_into(event_ingest_log_dsl::event_ingest_log)
    //     .values(new_logs)
    //     .execute(conn)
    //     .await?;
    //
    // Ok(())
    todo!()
}

pub fn game_and_raw_events(
    conn: &mut PgConnection,
    for_game_id: i64,
) -> QueryResult<(DbGame, Vec<(DbRawEvent, Vec<DbEventIngestLog>)>)> {
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;

    let game = games_dsl::games
        .filter(games_dsl::id.eq(for_game_id))
        .select(DbGame::as_select())
        .get_result::<DbGame>(conn)?;

    let raw_events = DbRawEvent::belonging_to(&game)
        .order_by(raw_events_dsl::game_event_index.asc())
        .load::<DbRawEvent>(conn)?;

    // This would be another belonging_to but diesel doesn't seem to support
    // compound foreign keys in associations
    let mut raw_logs = event_ingest_log_dsl::event_ingest_log
        .filter(event_ingest_log_dsl::game_id.eq(for_game_id))
        .order_by(event_ingest_log_dsl::game_event_index.asc())
        .then_order_by(event_ingest_log_dsl::log_index.asc())
        .get_results::<DbEventIngestLog>(conn)?
        .into_iter()
        .peekable();

    let logs_by_event = raw_events.iter()
        .map(|raw_event| {
            let mut events = Vec::new();
            while let Some(event) = raw_logs.next_if(|log| log.game_event_index == raw_event.game_event_index) {
                events.push(event);
            }
            events
        })
        .collect_vec();

    assert!(raw_logs.next().is_none(), "Failed to map all raw logs");

    let events_with_logs = raw_events
        .into_iter()
        .zip(logs_by_event)
        .collect::<Vec<_>>();

    Ok((game, events_with_logs))
}

pub struct Timings {
    pub check_already_ingested_duration: f64,
    pub parse_duration: f64,
    pub sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_fetch_for_check_duration: f64,
    pub events_for_game_timings: EventsForGameTimings,
    pub db_duration: f64,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub total_duration: f64,
}

pub fn insert_timings(
    conn: &mut PgConnection,
    for_game_id: i64,
    timings: Timings,
) -> QueryResult<()> {
    NewGameIngestTimings {
        game_id: for_game_id,
        check_already_ingested_duration: timings.check_already_ingested_duration,
        parse_duration: timings.parse_duration,
        sim_duration: timings.sim_duration,
        db_fetch_for_check_get_game_id_duration: timings
            .events_for_game_timings
            .get_game_id_duration,
        db_fetch_for_check_get_events_duration: timings.events_for_game_timings.get_events_duration,
        db_fetch_for_check_get_runners_duration: timings
            .events_for_game_timings
            .get_runners_duration,
        db_fetch_for_check_group_runners_duration: timings
            .events_for_game_timings
            .group_runners_duration,
        db_fetch_for_check_get_fielders_duration: timings
            .events_for_game_timings
            .get_fielders_duration,
        db_fetch_for_check_group_fielders_duration: timings
            .events_for_game_timings
            .group_fielders_duration,
        db_fetch_for_check_post_process_duration: timings
            .events_for_game_timings
            .post_process_duration,
        db_insert_duration: timings.db_insert_duration,
        db_fetch_for_check_duration: timings.db_fetch_for_check_duration,
        db_duration: timings.db_duration,
        check_round_trip_duration: timings.check_round_trip_duration,
        insert_extra_logs_duration: timings.insert_extra_logs_duration,
        total_duration: timings.total_duration,
    }
    .insert_into(crate::info_schema::info::game_ingest_timing::dsl::game_ingest_timing)
    .execute(conn)
    .map(|_| ())
}
