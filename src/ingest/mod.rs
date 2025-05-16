mod http;
mod sim;

use std::mem;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
pub use sim::{EventDetail, EventDetailFielder, EventDetailRunner, IngestLog};

use crate::db::Taxa;
use crate::ingest::sim::{Game, SimError};
use crate::{Db, db};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use log::{error, info, warn};
use mmolb_parsing::ParsedEventMessage;
use reqwest_middleware::ClientWithMiddleware;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::futures::FutureExt;
use rocket::{tokio, Orbit, Rocket, Shutdown};
use rocket::tokio::sync::{Mutex, Notify};
use rocket::tokio::task::JoinHandle;
use rocket_db_pools::Database;
use serde::Deserialize;
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestSetupError {
    #[error(transparent)]
    SetupConnectionAcquisitionFailed(
        #[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError,
    ),

    #[error(transparent)]
    TaxaSetupError(#[from] diesel::result::Error),

    #[error("Ingest task transitioned away from NotStarted before liftoff")]
    LeftNotStartedTooEarly,
}

#[derive(Debug, Error)]
pub enum IngestFatalError {
    #[error("The ingest task was interrupted while manipulating the task state")]
    InterruptedManipulatingTaskState,
    
    #[error("Ingest task transitioned to NotStarted state from some other state")]
    ReenteredNotStarted,
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
    state: Arc<Mutex<IngestTaskState>>,
}

impl IngestTask {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(IngestTaskState::NotStarted(Arc::new(Notify::new())))),
        }
    }
    
    pub async fn state(&self) -> IngestStatus {
        let task_state = self.state.lock().await;
        match &*task_state {
            IngestTaskState::NotStarted(_) => { IngestStatus::Starting }
            IngestTaskState::FailedToStart(err) => { IngestStatus::FailedToStart(err.to_string()) }
            IngestTaskState::Idle(_) => { IngestStatus::Idle }
            IngestTaskState::Running(_) => { IngestStatus::Running }
            IngestTaskState::ShutdownRequested => { IngestStatus::ShuttingDown }
            IngestTaskState::ExitedWithError(err) => { IngestStatus::ExitedWithError(err.to_string()) }
        }
    }
}

pub struct IngestFairing;

impl IngestFairing {
    pub fn new() -> Self { Self }
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
        let Some(pool) = Db::fetch(&rocket).cloned() else {
            error!("Cannot launch ingest task: Rocket is not managing a database pool!");
            return;
        };

        let Some(task) = rocket.state::<IngestTask>() else {
            error!("Cannot launch ingest task: Rocket is not managing an IngestTask!");
            return;
        };

        let notify = {
            let mut task_status = task.state.lock().await;
            if let IngestTaskState::NotStarted(notify) = &*task_status {
                notify.clone()
            } else {
                *task_status = IngestTaskState::FailedToStart(IngestSetupError::LeftNotStartedTooEarly);
                return;
            }
        };

        let shutdown = rocket.shutdown();
        let is_debug = rocket.config().profile == "debug";
        
        match launch_ingest_task(pool, is_debug, task.clone(), shutdown).await {
            Ok(handle) => {
                *task.state.lock().await = IngestTaskState::Idle(handle);
                notify.notify_waiters();
            }
            Err(err) => {
                *task.state.lock().await = IngestTaskState::FailedToStart(err);
            }
        };
    }

    async fn on_shutdown(&self, rocket: &Rocket<Orbit>) {
        if let Some(task) = rocket.state::<IngestTask>() {
            let mut state = task.state.lock().await;
            // Signal that we would like to shut down please
            let prev_state = mem::replace(&mut *state, IngestTaskState::ShutdownRequested);
            // If we have a join handle, try to shut down gracefully
            if let IngestTaskState::Idle(handle) | IngestTaskState::Running(handle) = prev_state {
                info!("Shutting down Ingest task");
                if let Err(e) = handle.await {
                    // Not much I can do about a failed join
                    error!("Failed to shut down Ingest task: {}", e);
                }
            } else {
                error!("Ingest task is in non-Idle or Running state at shutdown: {:?}", *state);
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

// This function sets up the ingest task, then returns a (TODO something)
// representing the execution of the ingest task. The intention is for
// errors in setup to be propagated to the caller, but errors in the
// task itself to be handled within the task
pub async fn launch_ingest_task(
    pool: Db,
    is_debug: bool,
    task: IngestTask,
    shutdown: Shutdown,
) -> Result<JoinHandle<()>, IngestSetupError> {
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

    Ok(tokio::spawn(ingest_task_runner(pool, taxa, client, is_debug, task, shutdown)))
}

pub async fn ingest_task_runner(pool: Db, taxa: Taxa, client: ClientWithMiddleware, is_debug: bool, task: IngestTask, mut shutdown: Shutdown) {
    loop {
        // Try to transition from idle to running, with lots of 
        // recovery behaviors
        loop {
            let mut task_state = task.state.lock().await;
            // We can't atomically move handle from idle to running, 
            // but we can replace it with the error that should happen
            // if something were to interrupt us. This should never be
            // seen by outside code, since we have the state locked.
            // It's just defensive programming.
            let prev_state = mem::replace(&mut *task_state, IngestTaskState::ExitedWithError(IngestFatalError::InterruptedManipulatingTaskState));
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
        
        let ingest_result = AssertUnwindSafe(do_ingest(&pool, &taxa, &client, is_debug))
            .catch_unwind()
            .await;

        if let Err(err) = ingest_result {
            warn!("Ingest task panicked! Error: {:?}", err);
        }

        // Try to transition from running to idle, with lots of 
        // recovery behaviors
        {
            let mut task_state = task.state.lock().await;
            // We can't atomically move handle from idle to running, 
            // but we can replace it with the error that should happen
            // if something were to interrupt us. This should never be
            // seen by outside code, since we have the state locked.
            // It's just defensive programming.
            let prev_state = mem::replace(&mut *task_state, IngestTaskState::ExitedWithError(IngestFatalError::InterruptedManipulatingTaskState));
            match prev_state {
                IngestTaskState::NotStarted(_) => {
                    *task_state = IngestTaskState::ExitedWithError(IngestFatalError::ReenteredNotStarted);
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
                    warn!("Ingest task state was Idle at the bottom of the loop (expected Running)");
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
        
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30 * 60)) => {},
            _ = &mut shutdown => { break; }
        }
    }
}

pub async fn do_ingest(pool: &Db, taxa: &Taxa, client: &ClientWithMiddleware, is_debug: bool) {
    // Temp override
    let is_debug = false;
    
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

    info!("Ingesting {} total games", games.len());

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
                info!(
                    "In debug mode, deleted game {} and all its events",
                    game_info.game_id
                );
            }
            false
        };

        if already_ingested {
            num_already_ingested_games_skipped += 1;
            continue;
        }

        let url = format!("https://mmolb.com/api/game/{}", game_info.game_id);
        let game_data = client
            .get(url)
            .send()
            .await
            .expect("TODO Error handling")
            .json::<mmolb_parsing::Game>()
            .await
            .expect("TODO Error handling");

        info!(
            "Fetched {} {} @ {} {} s{}d{}: {}",
            game_data.away_team_emoji,
            game_data.away_team_name,
            game_data.home_team_emoji,
            game_data.home_team_name,
            game_data.season,
            game_data.day,
            format!("https://mmolb.com/watch/{}", game_info.game_id),
        );

        // TODO Handle catch_unwind result better
        let result = AssertUnwindSafe(ingest_game(
            pool.clone(),
            &taxa,
            ingest_id,
            &game_info,
            game_data,
        ))
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
        "Marked ingest {ingest_id} finished. Time taken: {:#}.",
        HumanTime::from(ingest_end - ingest_start)
    );
}

// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    logs: Vec<(usize, IngestLog)>,
}

impl IngestLogs {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    pub fn critical(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((game_event_index, IngestLog {
            log_level: 0,
            log_text: s.into(),
        }));
    }

    pub fn error(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((game_event_index, IngestLog {
            log_level: 1,
            log_text: s.into(),
        }));
    }

    pub fn warn(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((game_event_index, IngestLog {
            log_level: 2,
            log_text: s.into(),
        }));
    }

    pub fn info(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((game_event_index, IngestLog {
            log_level: 3,
            log_text: s.into(),
        }));
    }

    pub fn debug(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((game_event_index, IngestLog {
            log_level: 4,
            log_text: s.into(),
        }));
    }

    pub fn trace(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((game_event_index, IngestLog {
            log_level: 5,
            log_text: s.into(),
        }));
    }

    pub fn into_vec(self) -> Vec<(usize, IngestLog)> {
        self.logs
    }
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

    let (mut game, game_creation_ingest_logs) = {
        let mut parsed_for_game = (&mut parsed).map(|(_, (parsed, _))| parsed);

        Game::new(&game_info.game_id, &mut parsed_for_game).expect("TODO Error handling")
    };

    let (detail_events, ingest_logs): (Vec<_>, Vec<_>) = parsed
        .map(|(index, (parsed, raw))| {
            let unparsed = parsed.clone().unparse();
            assert_eq!(unparsed, raw.message);
            // Sim has a different IngestLogs... this made sense at the time
            let mut ingest_logs = sim::IngestLogs::new();
            
            let event = match game.next(index, &parsed, &raw, &mut ingest_logs) {
                Ok(result) => { result }
                Err(e) => {
                    ingest_logs.critical(e.to_string());
                    None
                }
            };
            
            (event, ingest_logs.into_vec())
        })
        .unzip();

    // Take the None values out of detail_events
    let detail_events = detail_events
        .into_iter()
        .flat_map(|event| event)
        .collect::<Vec<_>>();

    // Scope to drop conn as soon as I'm done with it
    let (game_id, inserted_events) = {
        let mut conn = pool.get().await.expect("TODO Error handling");

        let game_id = db::insert_game(
            &mut conn,
            &taxa,
            ingest_id,
            &game_info.game_id,
            &game_data,
            game_creation_ingest_logs.into_iter().chain(ingest_logs),
            &detail_events,
        )
        .await
        .expect("TODO Error handling");

        // We can rebuild them
        let inserted_events = db::events_for_game(&mut conn, &taxa, &game_info.game_id)
            .await
            .expect("TODO Error handling");

        (game_id, inserted_events)
    };

    let mut extra_ingest_logs = IngestLogs::new();
    assert_eq!(inserted_events.len(), detail_events.len());
    for (reconstructed_detail, original_detail) in inserted_events.iter().zip(detail_events) {

        let index = reconstructed_detail.game_event_index;
        let fair_ball_index = reconstructed_detail.fair_ball_event_index;

        if let Some(index) = fair_ball_index {
            check_round_trip(
                index,
                &mut extra_ingest_logs,
                "contact event",
                &parsed_copy[index],
                &original_detail.to_parsed_contact(),
                &reconstructed_detail.to_parsed_contact(),
            );
        }

        check_round_trip(
            index,
            &mut extra_ingest_logs,
            "event",
            &parsed_copy[index],
            &original_detail.to_parsed(),
            &reconstructed_detail.to_parsed(),
        );
    }

    let extra_ingest_logs = extra_ingest_logs.into_vec();
    if !extra_ingest_logs.is_empty() {
        let mut conn = pool.get().await.expect("TODO Error handling");
        db::insert_additional_ingest_logs(&mut conn, game_id, extra_ingest_logs)
            .await
            .expect("TODO Error handling");
    }

    info!(
        "Finished importing {} {} @ {} {} s{}d{}",
        game_data.away_team_emoji,
        game_data.away_team_name,
        game_data.home_team_emoji,
        game_data.home_team_name,
        game_data.season,
        game_data.day,
    );
}

fn check_round_trip(
    index: usize,
    ingest_logs: &mut IngestLogs,
    label: &str,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &ParsedEventMessage<&str>,
    reconstructed_detail: &ParsedEventMessage<&str>,
) {
    if parsed != original_detail {
        ingest_logs.error(index, format!(
            "Round-trip of {} through EventDetail produced a mismatch:\n\
             Original: <pre>{:?}</pre>\
             Through EventDetail: <pre>{:?}</pre>",
            label,
            parsed,
            original_detail,
        ));
    }

    if parsed != reconstructed_detail {
        ingest_logs.error(index, format!(
            "Round-trip of {} through database produced a mismatch:\n\
             Original: <pre>{:?}</pre>\n\
             Through EventDetail: <pre>{:?}</pre>",
            label,
            parsed,
            reconstructed_detail,
        ));
    }
}
