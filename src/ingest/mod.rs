mod http;
mod sim;

pub use sim::EventDetail;

use crate::db::Taxa;
use crate::ingest::sim::Game;
use crate::{Db, db};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use log::info;
use reqwest_middleware::ClientWithMiddleware;
use rocket::tokio;
use rocket::tokio::task::JoinHandle;
use rocket_db_pools::diesel::prelude::*;
use rocket_db_pools::diesel::scoped_futures::ScopedFutureExt;
use serde::Deserialize;
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
) -> Result<JoinHandle<impl Future<Output = ()>>, IngestSetupError> {
    let client = http::get_caching_http_client();

    let taxa = {
        let mut conn = pool
            .get()
            .await
            .map_err(|err| IngestSetupError::SetupConnectionAcquisitionFailed(err))?;

        Taxa::new(&mut conn)
            .await
            .map_err(|err| IngestSetupError::TaxaSetupError(err))?
    };

    Ok(tokio::spawn(async move { ingest_task(pool, client, taxa) }))
}

pub async fn ingest_task(pool: Db, client: ClientWithMiddleware, taxa: Taxa) {
    let taxa_ref = &taxa; // Needed because of Rust's lacking lambda capture syntax

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

    // Override the cache policy: This is a live-changing endpoint and should
    // not be cached
    let games_response = client
        .get("https://freecashe.ws/api/games")
        // .with_extension(http_cache_reqwest::CacheMode::NoStore)
        .send()
        .await
        .expect("TODO Error handling");
    let games: GamesResponse = games_response.json().await.expect("TODO Error handling");

    info!("Running ingest on {} games", games.len());

    let mut num_incomplete_games_skipped = 0;
    let mut num_already_ingested_games_skipped = 0;

    for game_info in games {
        if game_info.state != GameState::Complete {
            num_incomplete_games_skipped += 1;
            continue;
        }

        let already_ingested = db::has_game(&game_info.game_id)
            .await
            .expect("TODO Error handling");

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

        // I'm adding enumeration to parsed, then stripping it out for 
        // the iterator fed to Game::new, on purpose. I need the 
        // counting to count every event, but I don't need the count
        // inside Game::new.
        let mut parsed = mmolb_parsing::process_events(&game_data)
            .into_iter()
            .zip(&game_data.event_log)
            .enumerate();

        let mut game = {
            let mut parsed_for_game = (&mut parsed)
                .map(|(_, (parsed, _))| parsed);

            Game::new(&game_info.game_id, &mut parsed_for_game)
                .expect("TODO Error handling")
        };

        info!(
            "Constructed game for for {} {} @ {} {} s{}d{}",
            game_data.away_team_emoji,
            game_data.away_team_name,
            game_data.home_team_emoji,
            game_data.home_team_name,
            game_data.season,
            game_data.day,
        );

        // I'm wrapping this in a transaction so I get errors
        // And a scope to drop conn at the appropriate time
        {
            let mut conn = pool.get().await.expect("TODO Error handling");

            conn.transaction::<_, diesel::result::Error, _>(|mut conn| {
                async move {
                    for (index, (parsed, raw)) in parsed {
                        info!("Applying event \"{}\"", raw.message);
                        
                        let detail = game.next(index, parsed).expect("TODO Error handling");

                        if let Some(detail) = detail {
                            db::insert_event(&mut conn, taxa_ref, ingest_id, &detail).await?;
                        }
                    }

                    Ok(())
                }
                .scope_boxed()
            })
            .await
            .expect("TODO Error handling");
        }
    }

    info!("{num_incomplete_games_skipped} incomplete games skipped");
    info!("{num_already_ingested_games_skipped} games already ingested");
}
