mod http;
mod sim;
mod chron;

use crate::db::{CompletedGameForDb, GameForDb, RowToEventError, Taxa};
use crate::ingest::sim::{Game, SimFatalError};
use crate::{Db, db};
use chrono::{DateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use log::{error, info, warn};
use mmolb_parsing::ParsedEventMessage;
use reqwest_middleware::ClientWithMiddleware;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::tokio::sync::{Notify, RwLock};
use rocket::tokio::task::JoinHandle;
use rocket::{Orbit, Rocket, Shutdown, tokio, figment};
use serde::Deserialize;
pub use sim::{EventDetail, EventDetailFielder, EventDetailRunner, IngestLog};
use std::{iter, mem};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use futures::{future, pin_mut, Stream, StreamExt};
use itertools::{Itertools, Either};
use thiserror::Error;
use crate::ingest::chron::{ChronEntities, ChronEntity, ChronError, ChronPage};

fn default_ingest_period() -> u64 {
    30 * 60  // 30 minutes, expressed in seconds
}

fn default_retries() -> u64 {
    3
}
fn default_chunks() -> usize {
    1000
}
fn default_fetch_rate_limit() -> u64 {
    10
}

#[derive(Clone, Deserialize)]
// Not sure if I need this because I also depend on serde, but it
// probably doesn't hurt
#[serde(crate = "rocket::serde")]
struct IngestConfig {
    #[serde(default = "default_ingest_period")]
    ingest_period_sec: u64,
    #[serde(default)]
    reimport_all_games: bool,
    #[serde(default)]
    start_ingest_every_launch: bool,
    #[serde(default)]
    cache_game_list_from_api: bool,
    #[serde(default = "default_retries")]
    fetch_game_list_retries: u64,
    #[serde(default = "default_chunks")]
    fetch_game_list_chunks: usize,
    #[serde(default)]
    cache_games_from_api: bool,
    #[serde(default = "default_retries")]
    fetch_games_retries: u64,
    #[serde(default = "default_fetch_rate_limit")]
    fetch_games_rate_limit_ms: u64,
    cache_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum IngestSetupError {
    // #[error("Failed to get a database connection for ingest setup due to {0}")]
    // DbPoolSetupError(#[from] rocket_sync_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error("Database error during ingest setup: {0}")]
    DbSetupError(#[from] diesel::result::Error),

    #[error("Ingest task transitioned away from NotStarted before liftoff")]
    LeftNotStartedTooEarly,
}

#[derive(Debug, Error)]
pub enum FetchGamesListError {
    #[error(transparent)]
    Network(#[from] reqwest_middleware::Error),

    #[error(transparent)]
    Deserialize(#[from] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum IngestFatalError {
    #[error("The ingest task was interrupted while manipulating the task state")]
    InterruptedManipulatingTaskState,

    #[error("Ingest task transitioned to NotStarted state from some other state")]
    ReenteredNotStarted,

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),

    #[error(transparent)]
    FetchGameError(#[from] FetchGameError),
    
    #[error("Couldn't open HTTP cache database: {0}")]
    OpenCacheDbError(#[from] sled::Error),
}

#[derive(Debug, Error)]
pub enum IngestDbError {
    // #[error(transparent)]
    // PoolError(#[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),
}

#[derive(Debug, Error)]
pub enum GameIngestFatalError {
    // #[error(transparent)]
    // PoolError(#[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),

    #[error(transparent)]
    SimFatalError(#[from] SimFatalError),
}

// This is the publicly visible version of IngestTaskState
#[derive(Debug)]
pub enum IngestStatus {
    Starting,
    FailedToStart(String),
    Idle,
    Running,
    ExitedWithError(String),
    ShuttingDown,
}

#[derive(Debug)]
enum IngestTaskState {
    // Ingest is not yet started. This state should be short-lived.
    // Notify is used to notify the task when the state has
    // transitioned to Idle.
    NotStarted(Arc<Notify>),
    // Ingest failed to start. This is a terminal state.
    FailedToStart(IngestSetupError),
    // Ingest task is alive, but not doing anything. The ingest task
    // runner must still make sure the thread exits shortly after the
    // state transitions to ShutdownRequested.
    Idle(JoinHandle<()>),
    // Ingest task is and doing an ingest. The ingest task runner must
    // still sure the thread exits shortly after the state transitions
    // to ShutdownRequested. The exit should be graceful, stopping the
    // current ingest.
    Running(JoinHandle<()>),
    // Rocket has requested a shutdown (e.g. due to ctrl-c). If the
    // task doesn't stop within a small time window, it will be killed.
    ShutdownRequested,
    // The ingest task has exited with a fatal error. This is only for
    // tasks which make the ingest
    ExitedWithError(IngestFatalError),
}

#[derive(Clone)]
pub struct IngestTask {
    state: Arc<RwLock<IngestTaskState>>,
}

impl IngestTask {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(IngestTaskState::NotStarted(Arc::new(
                Notify::new(),
            )))),
        }
    }

    pub async fn state(&self) -> IngestStatus {
        let task_state = self.state.read().await;
        match &*task_state {
            IngestTaskState::NotStarted(_) => IngestStatus::Starting,
            IngestTaskState::FailedToStart(err) => IngestStatus::FailedToStart(err.to_string()),
            IngestTaskState::Idle(_) => IngestStatus::Idle,
            IngestTaskState::Running(_) => IngestStatus::Running,
            IngestTaskState::ShutdownRequested => IngestStatus::ShuttingDown,
            IngestTaskState::ExitedWithError(err) => IngestStatus::ExitedWithError(err.to_string()),
        }
    }
}

pub struct IngestFairing;

impl IngestFairing {
    pub fn new() -> Self {
        Self
    }
}

#[rocket::async_trait]
impl Fairing for IngestFairing {
    fn info(&self) -> Info {
        Info {
            name: "Ingest",
            kind: Kind::Liftoff | Kind::Shutdown,
        }
    }

    async fn on_liftoff(&self, rocket: &Rocket<Orbit>) {
        let Some(pool) = Db::get_one(rocket).await else {
            error!("Cannot launch ingest task: Rocket is not managing a database pool!");
            return;
        };

        let Some(task) = rocket.state::<IngestTask>() else {
            error!("Cannot launch ingest task: Rocket is not managing an IngestTask!");
            return;
        };

        let Some(cache_path) = figment::providers::Env::var("HTTP_CACHE_DIR") else {
            error!("Cannot launch ingest task: HTTP_CACHE_DIR environment variable is not set");
            return;
        };
        
        let augmented_figment = rocket.figment()
            .clone() // Might be able to get away without this but. eh
            .join(("cache_path", cache_path));
        let config = match augmented_figment.extract() {
            Ok(config) => config,
            Err(err) => {
                error!(
                    "Cannot launch ingest task: Failed to parse ingest configuration: {}",
                    err
                );
                return;
            }
        };

        let notify = {
            let mut task_status = task.state.write().await;
            if let IngestTaskState::NotStarted(notify) = &*task_status {
                notify.clone()
            } else {
                *task_status =
                    IngestTaskState::FailedToStart(IngestSetupError::LeftNotStartedTooEarly);
                return;
            }
        };

        let shutdown = rocket.shutdown();

        match launch_ingest_task(pool, config, task.clone(), shutdown).await {
            Ok(handle) => {
                *task.state.write().await = IngestTaskState::Idle(handle);
                notify.notify_waiters();
            }
            Err(err) => {
                log::error!("Failed to launch ingest task: {}", err);
                *task.state.write().await = IngestTaskState::FailedToStart(err);
            }
        };
    }

    async fn on_shutdown(&self, rocket: &Rocket<Orbit>) {
        if let Some(task) = rocket.state::<IngestTask>() {
            // Signal that we would like to shut down please
            let prev_state = {
                let mut state = task.state.write().await;
                mem::replace(&mut *state, IngestTaskState::ShutdownRequested)
            };
            // If we have a join handle, try to shut down gracefully
            if let IngestTaskState::Idle(handle) | IngestTaskState::Running(handle) = prev_state {
                info!("Shutting down Ingest task");
                if let Err(e) = handle.await {
                    // Not much I can do about a failed join
                    error!("Failed to shut down Ingest task: {}", e);
                }
            } else {
                error!(
                    "Ingest task is in non-Idle or Running state at shutdown: {:?}",
                    prev_state
                );
            }
        } else {
            error!("Rocket is not managing an IngestTask!");
        };
    }
}

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

// This is incomplete, there is more data available in this response if necessary
#[derive(Debug, Deserialize)]
struct CashewsGameResponse {
    pub game_id: String,
    pub season: i64,
    pub day: i64,
    pub state: GameState,
}

#[derive(Deserialize)]

struct CashewsGamesResponse {
    items: Vec<CashewsGameResponse>,
    next_page: Option<String>,
}

// This function sets up the ingest task, then returns a JoinHandle for
// the ingest task. Errors in setup are propagated to the caller.
// Errors in the task itself are handled within the task.
async fn launch_ingest_task(
    db: Db,
    config: IngestConfig,
    task: IngestTask,
    shutdown: Shutdown,
) -> Result<JoinHandle<()>, IngestSetupError> {
    let client = http::get_caching_http_client();

    let (taxa, previous_ingest_start_time) = db.run(move |conn| {
        let taxa = Taxa::new(conn)?;

        let previous_ingest_start_time = if config.start_ingest_every_launch {
            None
        } else {
            db::latest_ingest_start_time(conn)?
                .map(|t| Utc.from_utc_datetime(&t))
        };

        Ok::<_, IngestSetupError>((taxa, previous_ingest_start_time))
    }).await?;

    Ok(tokio::spawn(ingest_task_runner(
        db,
        taxa,
        client,
        config,
        task,
        previous_ingest_start_time,
        shutdown,
    )))
}

async fn ingest_task_runner(
    db: Db,
    taxa: Taxa,
    client: ClientWithMiddleware,
    config: IngestConfig,
    task: IngestTask,
    mut previous_ingest_start_time: Option<DateTime<Utc>>,
    mut shutdown: Shutdown,
) {
    let ingest_period = Duration::from_secs(config.ingest_period_sec);
    loop {
        let (tag, ingest_start) = if let Some(prev_start) = previous_ingest_start_time {
            let next_start = prev_start + ingest_period;
            let wait_duration_chrono = next_start - Utc::now();

            // std::time::Duration can't represent negative durations.
            // Conveniently, the conversion from chrono to std also
            // performs the negativity check we would be doing anyway.
            match wait_duration_chrono.to_std() {
                Ok(wait_duration) => {
                    info!("Next ingest {}", HumanTime::from(wait_duration_chrono));
                    tokio::select! {
                        _ = tokio::time::sleep(wait_duration) => {},
                        _ = &mut shutdown => { break; }
                    }

                    let tag = format!("after waiting {}", HumanTime::from(wait_duration_chrono));
                    (tag, next_start)
                }
                Err(_) => {
                    // Indicates the wait duration was negative
                    let tag = format!(
                        "immediately (next scheduled ingest was {})",
                        HumanTime::from(next_start)
                    );
                    (tag, Utc::now())
                }
            }
        } else {
            let tag = "immediately (this is the first ingest)".to_string();
            (tag, Utc::now())
        };

        // I put this outside the `if` cluster intentionally, so the
        // compiler will error if I miss a code path
        info!("Starting ingest {tag}");
        previous_ingest_start_time = Some(ingest_start);

        // Try to transition from idle to running, with lots of
        // recovery behaviors
        loop {
            let mut task_state = task.state.write().await;
            // We can't atomically move handle from idle to running,
            // but we can replace it with the error that should happen
            // if something were to interrupt us. This should never be
            // seen by outside code, since we have the state locked.
            // It's just defensive programming.
            let prev_state = mem::replace(
                &mut *task_state,
                IngestTaskState::ExitedWithError(
                    IngestFatalError::InterruptedManipulatingTaskState,
                ),
            );
            match prev_state {
                IngestTaskState::NotStarted(notify) => {
                    // Put the value back how it was, drop the mutex, and wait.
                    // Once the wait is done we'll go round the loop again and
                    // this time should be in Idle state
                    let my_notify = notify.clone();
                    *task_state = IngestTaskState::NotStarted(notify);
                    drop(task_state);
                    my_notify.notified().await;
                }
                IngestTaskState::FailedToStart(err) => {
                    // Being in this state here is an oxymoron. Replacing the
                    // value and terminating this task is the least surprising
                    // way to react
                    *task_state = IngestTaskState::FailedToStart(err);
                    return;
                }
                IngestTaskState::Idle(handle) => {
                    // This is the one we want!
                    *task_state = IngestTaskState::Running(handle);
                    break;
                }
                IngestTaskState::Running(handle) => {
                    // Shouldn't happen, but if it does recovery is easy
                    warn!("Ingest task state was Running at the top of the loop (expected Idle)");
                    *task_state = IngestTaskState::Running(handle);
                    break;
                }
                IngestTaskState::ShutdownRequested => {
                    // Shutdown requested? Sure, we can do that.
                    *task_state = IngestTaskState::ShutdownRequested;
                    return;
                }
                IngestTaskState::ExitedWithError(err) => {
                    // This one is also an oxymoron
                    *task_state = IngestTaskState::ExitedWithError(err);
                    return;
                }
            }
        }

        let ingest_result = do_ingest(
            &db,
            &taxa,
            &client,
            &config,
            &mut shutdown,
            ingest_start,
        )
        .await;

        db.run(move |conn| {
            match ingest_result {
                Ok((ingest_id, last_completed_page)) => {
                    let ingest_end = Utc::now();
                    match db::mark_ingest_finished(conn, ingest_id, ingest_end, last_completed_page.as_deref()) {
                        Ok(()) => {
                            info!(
                                "Marked ingest {ingest_id} finished {:#}.",
                                HumanTime::from(ingest_end - ingest_start),
                            );
                        }
                        Err(err) => {
                            warn!("Failed to mark game ingest as finished: {}", err);
                        }
                    }
                }
                Err((err, ingest_id)) => {
                    if let Some(ingest_id) = ingest_id {
                        warn!(
                            "Fatal error in game ingest: {}. This ingest is aborted.",
                            err
                        );
                        match db::mark_ingest_aborted(conn, ingest_id, Utc::now()) {
                            Ok(()) => {}
                            Err(err) => {
                                warn!("Failed to mark game ingest as aborted: {}", err);
                            }
                        }
                    } else {
                        warn!(
                            "Fatal error in game ingest before adding ingest to database: {}. \
                            This ingest is aborted.",
                            err,
                        );
                    }
                }
            }
        }).await;

        // Try to transition from running to idle, with lots of
        // recovery behaviors
        {
            let mut task_state = task.state.write().await;
            // We can't atomically move handle from idle to running,
            // but we can replace it with the error that should happen
            // if something were to interrupt us. This should never be
            // seen by outside code, since we have the state locked.
            // It's just defensive programming.
            let prev_state = mem::replace(
                &mut *task_state,
                IngestTaskState::ExitedWithError(
                    IngestFatalError::InterruptedManipulatingTaskState,
                ),
            );
            match prev_state {
                IngestTaskState::NotStarted(_) => {
                    *task_state =
                        IngestTaskState::ExitedWithError(IngestFatalError::ReenteredNotStarted);
                    return;
                }
                IngestTaskState::FailedToStart(err) => {
                    // Being in this state here is an oxymoron. Replacing the
                    // value and terminating this task is the least surprising
                    // way to react
                    *task_state = IngestTaskState::FailedToStart(err);
                    return;
                }
                IngestTaskState::Running(handle) => {
                    // This is the one we want!
                    *task_state = IngestTaskState::Idle(handle);
                }
                IngestTaskState::Idle(handle) => {
                    // Shouldn't happen, but if it does recovery is easy
                    warn!(
                        "Ingest task state was Idle at the bottom of the loop (expected Running)"
                    );
                    *task_state = IngestTaskState::Idle(handle);
                }
                IngestTaskState::ShutdownRequested => {
                    // Shutdown requested? Sure, we can do that.
                    *task_state = IngestTaskState::ShutdownRequested;
                    return;
                }
                IngestTaskState::ExitedWithError(err) => {
                    // This one is also an oxymoron
                    *task_state = IngestTaskState::ExitedWithError(err);
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum FetchGameFromCacheError {
    #[error("Only-if-cached request failed with message {0}")]
    RequestError(reqwest_middleware::Error),

    #[error("Only-if-cached request returned non-success status code {0}")]
    NonSuccessStatusCode(reqwest::Error),

    #[error("Only-if-cached request failed to json decode with error {0}")]
    FailedJsonDecode(reqwest::Error),
}

async fn fetch_game_from_cache(
    client: &ClientWithMiddleware,
    url: &str,
) -> Result<mmolb_parsing::Game, FetchGameFromCacheError> {
    client
        .get(url)
        .with_extension(http_cache_reqwest::CacheMode::OnlyIfCached)
        .send()
        .await
        .map_err(FetchGameFromCacheError::RequestError)?
        .error_for_status()
        .map_err(FetchGameFromCacheError::NonSuccessStatusCode)?
        .json()
        .await
        .map_err(FetchGameFromCacheError::FailedJsonDecode)
}

#[derive(Debug, Error)]
enum FetchGamePartialError {
    #[error("Uncached request failed with message {0}")]
    RequestError(reqwest_middleware::Error),

    #[error("Uncached request returned non-success status code {0}")]
    NonSuccessStatusCode(reqwest::Error),

    #[error("Uncached request failed to json decode with error {0}")]
    FailedJsonDecode(reqwest::Error),
}

#[derive(Debug, Error)]
pub enum FetchGameError {
    #[error("Uncached request failed with message {0} after {1}")]
    RequestError(reqwest_middleware::Error, FetchGameFromCacheError),

    #[error("Uncached request returned non-success status code {0} after {1}")]
    NonSuccessStatusCode(reqwest::Error, FetchGameFromCacheError),

    #[error("Uncached request failed to json decode with error {0} after {1}")]
    FailedJsonDecode(reqwest::Error, FetchGameFromCacheError),
}

impl FetchGameError {
    fn from_partial(partial: FetchGamePartialError, cache_err: FetchGameFromCacheError) -> Self {
        match partial {
            FetchGamePartialError::RequestError(err) => {
                FetchGameError::RequestError(err, cache_err)
            }
            FetchGamePartialError::NonSuccessStatusCode(err) => {
                FetchGameError::NonSuccessStatusCode(err, cache_err)
            }
            FetchGamePartialError::FailedJsonDecode(err) => {
                FetchGameError::FailedJsonDecode(err, cache_err)
            }
        }
    }
}

async fn do_ingest(
    db: &Db,
    taxa: &Taxa,
    client: &ClientWithMiddleware,
    config: &IngestConfig,
    shutdown: &mut Shutdown,
    ingest_start: DateTime<Utc>,
) -> Result<(i64, Option<String>), (IngestFatalError, Option<i64>)> {
    info!("Ingest at {ingest_start} starting");

    // The only thing that errors here is opening the http cache, which
    // could be worked around by just not caching. I don't currently support
    // running without a cache but it could be added fairly easily.
    let chron = chron::Chron::new(&config.cache_path, config.fetch_game_list_chunks)
        .map_err(|err| (err.into(), None))?;

    info!("Initialized chron");

    // For lifetime reasons
    let reimport_all_games = config.reimport_all_games;
    let (start_page, ingest_id) = db.run(move |conn| {
        let start_page = if reimport_all_games {
            None
        } else {
            // TODO I think I'm storing the wrong page (but it'll just cause one redundant
            //   request, and not lose data, so it's not high priority to fix)
            db::last_completed_page(conn)
                .map_err(|e| (e.into(), None))?
        };

        info!("Got last completed page");

        let ingest_id = db::start_ingest(conn, ingest_start)
            .map_err(|e| (e.into(), None))?;

        info!("Inserted new ingest");

        Ok((start_page, ingest_id))
    }).await?;

    info!("Finished first db block");

    do_ingest_internal(
        db,
        taxa,
        client,
        config,
        &chron,
        shutdown,
        ingest_id,
        ingest_start,
        start_page,
    )
    .await
    .map_err(|e| (e.into(), Some(ingest_id)))
}

struct IngestStats {
    pub num_incomplete_games_skipped: usize,
    pub num_already_ingested_games_skipped: usize,
    pub num_games_imported: usize,
}

impl IngestStats {
    pub fn new() -> Self {
        Self {
            num_incomplete_games_skipped: 0,
            num_already_ingested_games_skipped: 0,
            num_games_imported: 0,
        }
    }

    pub fn add(&mut self, other: &IngestStats) {
        self.num_incomplete_games_skipped += other.num_incomplete_games_skipped;
        self.num_already_ingested_games_skipped += other.num_already_ingested_games_skipped;
        self.num_games_imported += other.num_games_imported;
    }
}

async fn do_ingest_internal(
    pool: &Db,
    taxa: &Taxa,
    client: &ClientWithMiddleware,
    config: &IngestConfig,
    chron: &chron::Chron,
    shutdown: &mut Shutdown,
    ingest_id: i64,
    ingest_start: DateTime<Utc>,
    start_page: Option<String>,
) -> Result<(i64, Option<String>), IngestFatalError> {
    info!("Recorded ingest start in database. Starting ingest...");

    // TODO Update last_completed_page
    let mut last_completed_page = start_page.clone();

    let mut stats = IngestStats::new();

    info!("Beginning ingest loop");

    let mut page_ingest_fut = None;
    let mut next_page_fut = Some(chron.games_page(start_page));

    loop {
        let page = match (page_ingest_fut.take(), next_page_fut.take()) {
            (None, None) => {
                todo!("I think this is an error state");
            }
            (None, Some(next_page_fut)) => {
                // This is the first iteration, so just wait for the page to be fetched
                tokio::select! {
                    page = next_page_fut => { page }
                    // TODO Ensure ingest is marked as aborted
                    _ = &mut *shutdown => { break; }
                }
            }
            (Some(page_ingest_fut), Some(next_page_fut)) => {
                // This is a middle iteration, we need to wait for the previous page to
                // ingest while simultaneously waiting for the next page to load
                // This is the first iteration, so just wait for the page to be fetched
                tokio::select! {
                    (page_stats, page) = future::join(page_ingest_fut, next_page_fut) => {
                        stats.add(&page_stats?);
                        page
                    }
                    // TODO Ensure ingest is marked as aborted
                    _ = &mut *shutdown => { break; }
                }
            }
            (Some(page_ingest_fut), None) => {
                // This is the last iteration, we just have to wait for the last
                // ingest to finish
                tokio::select! {
                    page_stats = page_ingest_fut => {
                        stats.add(&page_stats?);
                        break;
                    }
                    // TODO Ensure ingest is marked as aborted
                    _ = &mut *shutdown => { break; }
                }
            }
        };

        match page {
            Err(network_error) => {
                // TODO Ensure ingest is marked as aborted
                warn!("Stopping ingest early because of network error: {network_error}");
                break;
            }
            Ok(page) => {
                next_page_fut = Some(chron.games_page(page.next_page.clone()));
                page_ingest_fut = Some(ingest_page_of_games(pool, taxa, config, ingest_id, page));
            }
        }
    }

    info!("{} games ingested", stats.num_games_imported);
    info!("{} incomplete games skipped", stats.num_incomplete_games_skipped);
    info!("{} games already ingested", stats.num_already_ingested_games_skipped);

    Ok((ingest_id, last_completed_page))
}

fn prepare_completed_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Result<CompletedGameForDb, SimFatalError> {
    let parsed_game = mmolb_parsing::process_game(&entity.data);

    // I'm adding enumeration to parsed, then stripping it out for
    // the iterator fed to Game::new, on purpose. I need the
    // counting to count every event, but I don't need the count
    // inside Game::new.
    let mut parsed = parsed_game.iter().zip(&entity.data.event_log).enumerate();

    let (mut game, mut all_logs) = {
        let mut parsed_for_game = (&mut parsed).map(|(_, (parsed, _))| parsed);

        Game::new(&entity.entity_id, &entity.data, &mut parsed_for_game)?
    };

    let detail_events = parsed
        .map(|(game_event_index, (parsed, raw))| {
            // Sim has a different IngestLogs... this made sense at the time
            let mut ingest_logs = sim::IngestLogs::new(game_event_index);

            let unparsed = parsed.clone().unparse();
            if unparsed != raw.message {
                ingest_logs.error(format!(
                    "Round-trip of raw event through ParsedEvent produced a mismatch:\n\
                     Original: <pre>{:?}</pre>\n\
                     Through EventDetail: <pre>{:?}</pre>",
                    raw.message, unparsed,
                ));
            }

            let event = match game.next(game_event_index, &parsed, &raw, &mut ingest_logs) {
                Ok(result) => result,
                Err(e) => {
                    ingest_logs.critical(e.to_string());
                    None
                }
            };

            all_logs.push(ingest_logs.into_vec());

            event
        })
        .collect_vec();

    // Take the None values out of detail_events
    let detail_events = detail_events
        .into_iter()
        .flat_map(|event| event)
        .collect::<Vec<_>>();

    Ok(CompletedGameForDb {
        id: &entity.entity_id,
        raw_game: &entity.data,
        events: detail_events,
        logs: all_logs,
    })
}

async fn ingest_page_of_games<'t>(
    db: &Db,
    taxa: &'t Taxa,
    config: &IngestConfig,
    ingest_id: i64,
    page: ChronEntities<mmolb_parsing::Game>,
) -> Result<IngestStats, IngestFatalError> {
    // These clones are here because rocket_sync_db_pools' derived 
    // run() function imposes a 'static lifetime on its callback, and
    // thus we can't express "these variables must live longer than
    // this callback" to the type system. Cloning isn't the only 
    // solution (Arc would also work), but these objects are small
    // enough that cloning is a negligible-cost workaround to the
    // lifetime issue. They're also both frozen after launch, so
    // there's no risk of getting out of sync.
    // TODO Encapsulate these variables into some sort of Context
    //   object (and maybe take the opportunity to lift cache_path
    //   out of config).
    let taxa = taxa.clone();
    let config = config.clone();
    
    let stats = db.run(move |conn| {
        let raw_games = if !config.reimport_all_games {
            // Remove any games which are fully imported
            let is_finished = db::is_finished(conn, page.items.iter().map(|e| e.entity_id.as_str()).collect())?;
            iter::zip(page.items, is_finished)
                .flat_map(|(game, is_finished)| {
                    if is_finished {
                        None
                    } else {
                        Some(game)
                    }
                })
                .collect()
        } else {
            page.items
        };

        let games = raw_games.iter()
            .map(|entity| {
                Ok::<_, IngestFatalError>(if entity.data.state != "Complete" {
                    GameForDb::Incomplete {
                        game_id: &entity.entity_id,
                        raw_game: &entity.data,
                    }
                } else {
                    // TODO This used to be used a lot, which is why I precomputed it, but I think
                    //   it isn't. If that's true, remove it.
                    let description = format!(
                        "{} {} @ {} {} s{}d{}",
                        entity.data.away_team_emoji,
                        entity.data.away_team_name,
                        entity.data.home_team_emoji,
                        entity.data.home_team_name,
                        entity.data.season,
                        entity.data.day,
                    );

                    match prepare_completed_game_for_db(entity) {
                        Ok(game) => {
                            GameForDb::Completed { description, game }
                        }
                        Err(err) => {
                            // TODO Capture this on the games with issues page
                            warn!("Sim error importing {description}: {err}");
                            GameForDb::Incomplete {
                                game_id: &entity.entity_id,
                                raw_game: &entity.data,
                            }
                        }
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let num_incomplete_games_skipped = games.iter()
            .filter(|g| {
                match g {
                    GameForDb::Incomplete { .. } => { true }
                    GameForDb::Completed { .. } => { false }
                }
            })
            .count();
        let num_already_ingested_games_skipped = raw_games.len() - games.len();
        let num_games_imported = games.len() - num_incomplete_games_skipped;
        let total_games = games.len();
        info!("Ingesting {} games, ignoring {} already-ingested games", games.len(), raw_games.len() - games.len());

        // Insert the games into the database
        db::insert_games(
            conn,
            &taxa,
            ingest_id,
            games,
        )?;
        
        info!("Chunk ingested");
        
        // Immediately turn around and fetch all the games we just inserted,
        // so we can verify that they round-trip correctly.
        // This step, and all the following verification steps, could be 
        // skipped. However, my profiling shows that it's negligible
        // cost so I haven't added the capability.
        let mmolb_game_ids = games.iter()
            .filter_map(|game| match game {
                GameForDb::Incomplete { .. } => { None }
                GameForDb::Completed { game, .. } => { Some(game.id) }
            })
            .collect_vec();
        
        let (ingested_games, timings) = db::events_for_games(conn, &taxa, mmolb_game_ids)?;
        assert_eq!(total_games, ingested_games.len());

        Ok::<_, IngestFatalError>(IngestStats {
            num_incomplete_games_skipped,
            num_already_ingested_games_skipped,
            num_games_imported,
        })
    }).await?;

    // TODO Update all the following for batched insert
    // let inserted_events = db::events_for_game(conn, &taxa, mmolb_game_id).await?;
    // let mut extra_ingest_logs = IngestLogs::new();
    // if inserted_events.len() != detail_events.len() {
    //     error!(
    //         "Number of events read from the db ({}) does not match number of events written to \
    //         the db ({})",
    //         inserted_events.len(),
    //         detail_events.len(),
    //     );
    // }
    // for (reconstructed_detail, original_detail) in inserted_events.iter().zip(detail_events) {
    //     let index = original_detail.game_event_index;
    //     let fair_ball_index = original_detail.fair_ball_event_index;
    //
    //     if let Some(index) = fair_ball_index {
    //         check_round_trip(
    //             index,
    //             &mut extra_ingest_logs,
    //             true,
    //             &parsed_copy[index],
    //             &original_detail,
    //             reconstructed_detail,
    //         );
    //     }
    //
    //     check_round_trip(
    //         index,
    //         &mut extra_ingest_logs,
    //         false,
    //         &parsed_copy[index],
    //         &original_detail,
    //         reconstructed_detail,
    //     );
    // }
    // let check_round_trip_duration = (Utc::now() - check_round_trip_start).as_seconds_f64();
    //
    // let insert_extra_logs_start = Utc::now();
    // let extra_ingest_logs = extra_ingest_logs.into_vec();
    // if !extra_ingest_logs.is_empty() {
    //     db::insert_additional_ingest_logs(conn, game_id, extra_ingest_logs).await?;
    // }
    // let insert_extra_logs_duration = (Utc::now() - insert_extra_logs_start).as_seconds_f64();
    //
    // {
    //     db::insert_timings(
    //         conn,
    //         game_id,
    //         db::Timings {
    //             check_already_ingested_duration: pre_ingest_timings.check_already_ingested_duration,
    //             parse_duration,
    //             sim_duration,
    //             db_insert_duration,
    //             db_fetch_for_check_duration,
    //             events_for_game_timings,
    //             db_duration,
    //             check_round_trip_duration,
    //             insert_extra_logs_duration,
    //             total_duration: (Utc::now() - pre_ingest_timings.total_start).as_seconds_f64(),
    //         },
    //     )
    //         .await?;
    // }
    //
    // info!(
    //     "Finished importing {} {} @ {} {} s{}d{}",
    //     game_data.away_team_emoji,
    //     game_data.away_team_name,
    //     game_data.home_team_emoji,
    //     game_data.home_team_name,
    //     game_data.season,
    //     game_data.day,
    // );
    
    Ok(stats)
}


// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    logs: Vec<IngestLog>,
}

impl IngestLogs {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    #[allow(dead_code)]
    pub fn critical(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index,
            log_level: 0,
            log_text: s.into(),
        });
    }

    pub fn error(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index,
            log_level: 1,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn warn(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index,
            log_level: 2,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn info(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index,
            log_level: 3,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn debug(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index,
            log_level: 4,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index,
            log_level: 5,
            log_text: s.into(),
        });
    }

    pub fn into_vec(self) -> Vec<IngestLog> {
        self.logs
    }
}

struct PreIngestTimings {
    pub total_start: DateTime<Utc>,
    pub check_already_ingested_duration: f64,
}


fn log_if_error<'g, E: std::fmt::Display>(
    ingest_logs: &mut IngestLogs,
    index: usize,
    to_parsed_result: Result<ParsedEventMessage<&'g str>, E>,
    log_prefix: &str,
) -> Option<ParsedEventMessage<&'g str>> {
    match to_parsed_result {
        Ok(to_contact_result) => Some(to_contact_result),
        Err(err) => {
            ingest_logs.error(index, format!("{log_prefix}: {err}"));
            None
        }
    }
}

// The particular combination of &str and String type arguments is
// dictated by the caller
fn check_round_trip(
    index: usize,
    ingest_logs: &mut IngestLogs,
    is_contact_event: bool,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &EventDetail<&str>,
    reconstructed_detail: &Result<EventDetail<String>, RowToEventError>,
) {
    let Some(parsed_through_detail) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    }) else {
        return;
    };

    if parsed != &parsed_through_detail {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through EventDetail produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\
                 Through EventDetail: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_detail,
            ),
        );
    }

    let reconstructed_detail = match reconstructed_detail {
        Ok(reconstructed_detail) => reconstructed_detail,
        Err(err) => {
            ingest_logs.error(
                index,
                format!(
                    "Attempt to round-trip {} ParsedEventMessage -> EventDetail -> database \
                    -> EventDetail -> ParsedEventMessage failed at the database -> EventDetail \
                    step with error: {err}",
                    if is_contact_event {
                        "contact event"
                    } else {
                        "event"
                    }
                ),
            );
            return;
        }
    };

    let Some(parsed_through_db) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            database -> EventDetail -> ParsedEventMessage failed at the EventDetail -> \
            ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> database \
            -> EventDetail -> ParsedEventMessage failed at the EventDetail -> ParsedEventMessage \
            step with error",
        )
    }) else {
        return;
    };

    if parsed != &parsed_through_db {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through database produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\n\
                 Through database: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_db,
            ),
        );
    }
}
