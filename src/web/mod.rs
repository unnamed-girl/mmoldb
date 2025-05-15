mod error;
mod utility_contexts;

use rocket::{get, routes, uri};
use rocket_db_pools::Connection;
use rocket_db_pools::diesel::AsyncConnection;
use rocket_db_pools::diesel::scoped_futures::ScopedFutureExt;
use rocket_dyn_templates::{Template, context};
use serde::Serialize;

use crate::{Db, db};
use error::AppError;
use utility_contexts::FormattedDateContext;

#[get("/game/<game_id>")]
async fn game_page(game_id: i64, mut db: Connection<Db>) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct EventContext {
        text: String,
    }
    
    #[derive(Serialize)]
    struct GameContext {
        id: String,
        watch_uri: String,
        api_uri: String,
        season: i32,
        day: i32,
        away_team_emoji: String,
        away_team_name: String,
        home_team_emoji: String,
        home_team_name: String,
        events: Vec<EventContext>
    }

    let (game, events) = db::game_and_raw_events(&mut db, game_id).await?;
    let watch_uri = format!("https://mmolb.com/watch/{}", game.mmolb_game_id);
    let api_uri = format!("https://mmolb.com/api/game/{}", game.mmolb_game_id);
    let game = GameContext {
        id: game.mmolb_game_id,
        watch_uri,
        api_uri,
        season: game.season,
        day: game.day,
        away_team_emoji: game.away_team_emoji,
        away_team_name: game.away_team_name,
        home_team_emoji: game.home_team_emoji,
        home_team_name: game.home_team_name,
        events: events.into_iter().map(|event| EventContext {
            text: event.event_text,
        }).collect(),
    };

    Ok(Template::render("game", context! { game: game }))
}

#[get("/ingest/<ingest_id>")]
async fn ingest_page(ingest_id: i64, mut db: Connection<Db>) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct GameContext {
        uri: String,
        season: i32,
        day: i32,
        away_team_emoji: String,
        away_team_name: String,
        home_team_emoji: String,
        home_team_name: String,
    }

    #[derive(Serialize)]
    struct IngestContext {
        id: i64,
        started_at: FormattedDateContext,
        finished_at: Option<FormattedDateContext>,
        games: Vec<GameContext>,
    }

    let (ingest, games) = db::ingest_with_games(&mut db, ingest_id).await?;
    let ingest = IngestContext {
        id: ingest.id,
        started_at: (&ingest.date_started).into(),
        finished_at: ingest.date_finished.as_ref().map(Into::into),
        games: games
            .into_iter()
            .map(|game| GameContext {
                uri: uri!(game_page(game.id)).to_string(),
                season: game.season,
                day: game.day,
                away_team_emoji: game.away_team_emoji,
                away_team_name: game.away_team_name,
                home_team_emoji: game.home_team_emoji,
                home_team_name: game.home_team_name,
            })
            .collect(),
    };

    Ok(Template::render("ingest", context! { ingest: ingest }))
}

#[get("/")]
async fn index(mut db: Connection<Db>) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct IngestContext {
        uri: String,
        num_games: i64,
        started_at: FormattedDateContext,
        finished_at: Option<FormattedDateContext>,
    }

    // A transaction is probably overkill for this, but it's
    // TECHNICALLY the only correct way to make sure that the
    // value of number_of_ingests_not_shown is correct
    let (total_num_ingests, displayed_ingests) = db
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                let num = db::ingest_count(conn).await?;
                let ingests = db::latest_ingests(conn).await?;
                Ok((num, ingests))
            }
            .scope_boxed()
        })
        .await?;

    let number_of_ingests_not_shown = total_num_ingests - displayed_ingests.len() as i64;
    let ingests: Vec<_> = displayed_ingests
        .into_iter()
        .map(|(ingest, num_games)| IngestContext {
            uri: uri!(ingest_page(ingest.id)).to_string(),
            num_games,
            started_at: (&ingest.date_started).into(),
            finished_at: ingest.date_finished.as_ref().map(Into::into),
        })
        .collect();

    Ok(Template::render(
        "index",
        context! {
            ingests: ingests,
            number_of_ingests_not_shown: number_of_ingests_not_shown,
        },
    ))
}

pub fn routes() -> Vec<rocket::Route> {
    routes![index, ingest_page, game_page]
}
