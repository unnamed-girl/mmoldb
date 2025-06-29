use crate::db::{weather, GameForDb};
use crate::models::{DbWeather, NewWeather};
use diesel::connection::DefaultLoadingMode;
use diesel::prelude::*;
use diesel::{QueryResult, RunQueryDsl};
use diesel::result::DatabaseErrorKind;
use diesel::result::Error::DatabaseError;
use hashbrown::{Equivalent, HashMap};
use itertools::Itertools;
use log::{info, warn};

// Uses hashbrown::HashMap because the std HashMap has a limitation on
// tuples of references
#[derive(Debug, Hash, Eq, PartialEq)]
pub(super) struct UniqueWeather {
    name: String,
    emoji: String,
    tooltip: String,
}

impl Into<UniqueWeather> for DbWeather {
    fn into(self) -> UniqueWeather {
        UniqueWeather {
            name: self.name,
            emoji: self.emoji,
            tooltip: self.tooltip,
        }
    }
}

impl Equivalent<UniqueWeather> for (&str, &str, &str) {
    fn equivalent(&self, key: &UniqueWeather) -> bool {
        self.0 == key.name && self.1 == key.emoji && self.2 == key.tooltip
    }
}

pub(super) type WeatherTable = HashMap<UniqueWeather, i64>;

pub(super) fn create_weather_table(conn: &mut PgConnection, games: &[GameForDb]) -> QueryResult<WeatherTable> {
    let mut retries_remaining = 10;
    let weather_table = loop {
        match create_weather_table_inner(conn, games) {
            Ok(weather_table) => break weather_table,
            Err(e) => {
                match e {
                    DatabaseError(DatabaseErrorKind::CheckViolation, err) if err.constraint_name() == Some("weather_name_emoji_tooltip_key") => {
                        if retries_remaining <= 0 {
                            warn!("Constraint violation building weather table. No retries remaining.");
                            // Must rebuild the error
                            return Err(DatabaseError(DatabaseErrorKind::CheckViolation, err));
                        }

                        warn!(
                            "Unique constraint violation building weather table. Trying again \
                            ({retries_remaining} retries remaining). Error: {}",
                            err.message(),
                        );

                        retries_remaining -= 1;
                        // And continue the loop
                    }
                    other_err => return Err(other_err),

                }
            }
        };
    };

    Ok(weather_table)
}

pub(super) fn create_weather_table_inner(conn: &mut PgConnection, games: &[GameForDb]) -> QueryResult<WeatherTable> {
    // Get or create weather
    // Note: Based on what I read online, trying an insert and catching unique
    // violations causes db bloat. There are (surprisingly complicated) ways to
    // do a get-or-create in one query, but for simplicity I'm just going to
    // do it as two queries and rely on the transaction to handle conflicts

    // Get all weathers. Weather table shouldn't be too big so this
    // should be fine. TODO Cache weather LUT
    let mut weather_table = crate::data_schema::data::weather::dsl::weather
        .select(DbWeather::as_select())
        .load_iter::<DbWeather, DefaultLoadingMode>(conn)?
        .map_ok(|weather| {
            let id = weather.id; // Lifetime reasons
            (weather.into(), id)
        })
        .collect::<QueryResult<HashMap<UniqueWeather, i64>>>()?;
    
    info!("Read weather table: {:#?}", weather_table);

    let new_weathers = games.iter()
        .map(GameForDb::raw)
        .filter_map(|(_, raw_game)| {
            if weather_table.contains_key(&(
                raw_game.weather.name.as_str(),
                raw_game.weather.emoji.as_str(),
                raw_game.weather.tooltip.as_str(),
            )) {
                None
            } else {
                Some(NewWeather {
                    name: &raw_game.weather.name,
                    emoji: &raw_game.weather.emoji,
                    tooltip: &raw_game.weather.tooltip,
                })
            }
        })
        .unique_by(|w| (w.name, w.emoji, w.tooltip))
        .collect_vec();

    info!("Need to insert new weathers: {:#?}", new_weathers);

    let inserted_weathers = diesel::insert_into(crate::data_schema::data::weather::dsl::weather)
        .values(&new_weathers)
        .returning(DbWeather::as_returning())
        .get_results::<DbWeather>(conn)?;

    info!("Inserted new weathers: {:#?}", inserted_weathers);

    weather_table.extend(
        inserted_weathers.into_iter()
            .map(|weather| {
                let id = weather.id; // Lifetime reasons
                (weather.into(), id)
            })
    );

    info!("Updated weather table: {:#?}", weather_table);

    Ok(weather_table)
}