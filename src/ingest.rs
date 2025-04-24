use chrono::{DateTime, Utc};
use chrono::serde::ts_milliseconds;
use log::info;
use crate::{db, Db};
use serde::{Deserialize};

#[derive(Deserialize)]
enum GameState {
    Complete,
    Pitch,
}

#[derive(Deserialize)]
struct GameResponse {
    game_id: String,
    season: i64,
    day: i64,
    home_team_id: String,
    away_team_id: String,
    #[serde(with = "ts_milliseconds")]
    last_update: DateTime<Utc>,
    state: GameState,
}

type GamesResponse = Vec<GameResponse>;

pub async fn ingest_task(pool: Db) -> () {
    let ingest_start = Utc::now();
    info!("Ingest at {ingest_start} started");

    let mut conn = pool.get().await
        .expect("TODO Error handling");
    db::start_ingest(ingest_start, &mut conn).await
        .expect("TODO Error handling");

    let games_response = reqwest::get("https://freecashe.ws/api/games").await
        .expect("TODO Error handling");
    let games: GamesResponse = games_response.json().await
        .expect("TODO Error handling");

    info!("Running ingest on {} games", games.len());
}