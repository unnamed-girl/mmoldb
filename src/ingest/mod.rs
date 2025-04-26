mod http;
mod sim;

use chrono::{DateTime, Utc};
use chrono::serde::ts_milliseconds;
use log::info;
use crate::{db, Db};
use serde::Deserialize;
use crate::ingest::sim::Game;

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

pub async fn ingest_task(pool: Db) -> () {
    let client = http::get_caching_http_client();

    // ------------ This is where the loop will start -------------

    let ingest_start = Utc::now();
    info!("Ingest at {ingest_start} started");

    let mut conn = pool.get().await
        .expect("TODO Error handling");
    db::start_ingest(ingest_start, &mut conn).await
        .expect("TODO Error handling");
    
    info!("Recorded ingest start in database");

    // Override the cache policy: This is a live-changing endpoint and should
    // not be cached
    let games_response = client.get("https://freecashe.ws/api/games")
        // .with_extension(http_cache_reqwest::CacheMode::NoStore)
        .send()
        .await
        .expect("TODO Error handling");
    let games: GamesResponse = games_response.json().await
        .expect("TODO Error handling");

    info!("Running ingest on {} games", games.len());

    let mut num_incomplete_games_skipped = 0;
    let mut num_already_ingested_games_skipped = 0;

    for game_info in games {
        if game_info.state != GameState::Complete {
            num_incomplete_games_skipped += 1;
            continue;
        }

        let already_ingested = db::has_game(&game_info.game_id).await
            .expect("TODO Error handling");

        if already_ingested {
            num_already_ingested_games_skipped += 1;
            continue;
        }

        let url = format!("https://mmolb.com/api/game/{}", game_info.game_id);
        info!("Fetching {url}");
        let game_data = client.get(url)
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

        let parsed = mmolb_parsing::process_events(&game_data);

        // I'm wrapping this in a transaction so I get errors 
        let mut game = Game::new();
        for event in parsed {
            game.apply(&event)
                .expect("TODO Error handling");
        }
    }

    info!("{num_incomplete_games_skipped} incomplete games skipped");
    info!("{num_already_ingested_games_skipped} games already ingested");
}