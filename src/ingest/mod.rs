mod http;
mod sim;

pub use sim::{EventDetail, EventDetailFielder, EventDetailRunner};

use crate::db::Taxa;
use crate::ingest::sim::Game;
use crate::{Db, db};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use log::info;
use mmolb_parsing::ParsedEventMessage;
use rocket::futures::FutureExt;
use rocket::tokio;
use rocket::tokio::task::JoinHandle;
use serde::Deserialize;
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub enum GameState {
    Complete,
    Pitch,
    Field,
    Change,
    NowBatting,
    Change2, // what
    MoundVisit,
    GameOver,
    Recordkeeping,
    LiveNow,
    PitchingMatchup,
    AwayLineup,
    HomeLineup,
    PlayBall,
    InningStart,
    InningEnd,
}

// Allowing dead code because I want to keep the unused members of this
// struct so that it's easy to see that I have them if I find a use for
// them.
#[allow(dead_code)]
#[derive(Deserialize)]
struct CashewsGameResponse {
    pub game_id: String,
    pub season: i64,
    pub day: i64,
    pub home_team_id: String,
    pub away_team_id: String,
    #[serde(with = "ts_milliseconds")]
    pub last_update: DateTime<Utc>,
    pub state: GameState,
}

type GamesResponse = Vec<CashewsGameResponse>;

#[derive(Debug, Error)]
pub enum IngestSetupError {
    #[error(transparent)]
    SetupConnectionAcquisitionFailed(
        #[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError,
    ),

    #[error(transparent)]
    TaxaSetupError(#[from] diesel::result::Error),
}

// This function sets up the ingest task, then returns a (TODO something)
// representing the execution of the ingest task. The intention is for
// errors in setup to be propagated to the caller, but errors in the
// task itself to be handled within the task
pub async fn launch_ingest_task(
    pool: Db,
    is_debug: bool,
) -> Result<JoinHandle<impl Future<Output = ()>>, IngestSetupError> {
    Ok(tokio::spawn(async move { ingest_task(pool, is_debug) }))
}

pub async fn ingest_task(pool: Db, is_debug: bool) {
    let is_debug = false; // TEMPORARY
    
    let client = http::get_caching_http_client();

    let taxa = {
        let mut conn = pool
            .get()
            .await
            .map_err(|err| IngestSetupError::SetupConnectionAcquisitionFailed(err))
            .expect("TODO Error handling");

        Taxa::new(&mut conn)
            .await
            .map_err(|err| IngestSetupError::TaxaSetupError(err))
            .expect("TODO Error handling")
    };

    // ------------ This is where the loop will start -------------

    let ingest_start = Utc::now();
    info!("Ingest at {ingest_start} started");

    // Introduce a scope so `conn` is dropped as soon as we finish with it
    let ingest_id = {
        let mut conn = pool.get().await.expect("TODO Error handling");
        db::start_ingest(&mut conn, ingest_start)
            .await
            .expect("TODO Error handling")
    };

    info!("Recorded ingest start in database");

    let games_request = if is_debug {
        // In development we want to always cache this
        client.get("https://freecashe.ws/api/games")
    } else {
        // Override the cache policy: This is a live-changing endpoint and should
        // not be cached
        client
            .get("https://freecashe.ws/api/games")
            .with_extension(http_cache_reqwest::CacheMode::NoStore)
    };

    let games_response = games_request.send().await.expect("TODO Error handling");

    let games: GamesResponse = games_response.json().await.expect("TODO Error handling");

    info!("Running ingest on {} games", games.len());

    let mut num_incomplete_games_skipped = 0;
    let mut num_already_ingested_games_skipped = 0;

    for game_info in games {
        if game_info.state != GameState::Complete {
            num_incomplete_games_skipped += 1;
            continue;
        }

        let already_ingested = if !is_debug {
            let mut conn = pool.get().await.expect("TODO Error handling");
            db::has_game(&mut conn, &game_info.game_id)
                .await
                .expect("TODO Error handling")
        } else {
            let mut conn = pool.get().await.expect("TODO Error handling");
            let num_deleted = db::delete_game(&mut conn, &game_info.game_id)
                .await
                .expect("TODO Error handling");

            if num_deleted > 0 {
                info!("In debug mode, deleted game {} and all its events", game_info.game_id);
            }
            false
        };

        if already_ingested {
            num_already_ingested_games_skipped += 1;
            continue;
        }

        let url = format!("https://mmolb.com/api/game/{}", game_info.game_id);
        info!("Fetching {url}");
        let game_data = client
            .get(url)
            .send()
            .await
            .expect("TODO Error handling")
            .json::<mmolb_parsing::Game>()
            .await
            .expect("TODO Error handling");

        info!(
            "Got data for {} {} @ {} {} s{}d{}",
            game_data.away_team_emoji,
            game_data.away_team_name,
            game_data.home_team_emoji,
            game_data.home_team_name,
            game_data.season,
            game_data.day,
        );

        // I think cloning pool is the intended behavior
        let result = std::panic::AssertUnwindSafe(ingest_game(pool.clone(), &taxa, ingest_id, &game_info, game_data))
            .catch_unwind()
            .await;
        
        if result.is_err() {
            break;
        }
    }

    info!("{num_incomplete_games_skipped} incomplete games skipped");
    info!("{num_already_ingested_games_skipped} games already ingested");

    let ingest_end = Utc::now();
    {
        let mut conn = pool.get().await.expect("TODO Error handling");
        db::mark_ingest_finished(&mut conn, ingest_id, ingest_end)
            .await
            .expect("TODO Error handling")
    }
    info!(
        "Marked ingest {ingest_id} finished {:#}.",
        HumanTime::from(ingest_end - ingest_start)
    );
}

async fn ingest_game(
    pool: Db,
    taxa: &Taxa,
    ingest_id: i64,
    game_info: &CashewsGameResponse,
    game_data: mmolb_parsing::Game,
) {
    let parsed_copy = mmolb_parsing::process_game(&game_data);

    // I'm adding enumeration to parsed, then stripping it out for
    // the iterator fed to Game::new, on purpose. I need the
    // counting to count every event, but I don't need the count
    // inside Game::new.
    let mut parsed = parsed_copy.iter().zip(&game_data.event_log).enumerate();

    let mut game = {
        let mut parsed_for_game = (&mut parsed).map(|(_, (parsed, _))| parsed);

        Game::new(&game_info.game_id, &mut parsed_for_game).expect("TODO Error handling")
    };

    info!(
        "Constructed game for {} {} @ {} {} s{}d{}",
        game_data.away_team_emoji,
        game_data.away_team_name,
        game_data.home_team_emoji,
        game_data.home_team_name,
        game_data.season,
        game_data.day,
    );

    let detail_events: Vec<_> = parsed
        .flat_map(|(index, (parsed, raw))| {
            let unparsed = parsed.clone().unparse();
            assert_eq!(unparsed, raw.message);

            info!(
                "Applying event {:#?} \"{}\"",
                parsed.discriminant(),
                raw.message
            );

            game.next(index, &parsed, &raw)
                .expect("TODO Error handling")
        })
        .collect();

    // Scope to drop conn as soon as I'm done with it
    let inserted_events = {
        let mut conn = pool.get().await.expect("TODO Error handling");

        db::insert_game(&mut conn, &taxa, ingest_id, &game_info.game_id, &game_data, &detail_events)
            .await
            .expect("TODO Error handling");

        // We can rebuild them
        db::events_for_game(&mut conn, &taxa, &game_info.game_id)
            .await
            .expect("TODO Error handling")
    };

    info!(
        "Checking round-trip for https://mmolb.com/watch/{}",
        game_info.game_id
    );

    assert_eq!(inserted_events.len(), detail_events.len());
    for (reconstructed_detail, original_detail) in inserted_events.iter().zip(detail_events) {
        info!(
            "Original Baserunners:      {:?}\nReconstructed baserunners: {:?}",
            original_detail.baserunners, reconstructed_detail.baserunners,
        );

        let index = reconstructed_detail.game_event_index;
        let fair_ball_index = reconstructed_detail.fair_ball_event_index;

        if let Some(index) = fair_ball_index {
            check_round_trip(
                "Contact event",
                &parsed_copy[index],
                &original_detail.to_parsed_contact(),
                &reconstructed_detail.to_parsed_contact(),
            );
        }

        check_round_trip(
            "Event",
            &parsed_copy[index],
            &original_detail.to_parsed(),
            &reconstructed_detail.to_parsed(),
        );
    }
}

fn check_round_trip(
    label: &str,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &ParsedEventMessage<&str>,
    reconstructed_detail: &ParsedEventMessage<&str>,
) {
    info!(
        "{}\n           Original: {:?}\
           \nThrough EventDetail: {:?}\
           \n         Through db: {:?}",
        parsed.clone().unparse(),
        parsed,
        original_detail,
        reconstructed_detail,
    );

    // The linter incorrectly marks `label` as dead code even though
    // it's used in the assert statements. This lets me silence only
    // that warning without having to silence dead_code in the whole
    // function.
    #[allow(path_statements)]
    label;

    assert_eq!(
        parsed, original_detail,
        "{label} round-trip through EventDetail failed (left is original, right is reconstructed)"
    );

    assert_eq!(
        parsed, reconstructed_detail,
        "{label} round-trip through db failed (left is original, right is reconstructed)"
    );
}
