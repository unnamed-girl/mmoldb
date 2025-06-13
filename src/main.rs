mod db;
#[rustfmt::skip] // This is a generated file
mod data_schema;
#[rustfmt::skip] // This is a generated file
mod taxa_schema;
#[rustfmt::skip] // This is a generated file
mod info_schema;
mod ingest;
mod models;
mod web;

use crate::ingest::{IngestFairing, IngestTask};
use num_format::{Locale, ToFormattedString};
use rocket::fairing::AdHoc;
use rocket::{Build, Rocket, launch};
use rocket_db_pools::diesel::prelude::*;
use rocket_db_pools::{Database, diesel::PgPool};
use rocket_dyn_templates::Template;
use rocket_dyn_templates::tera::Value;
use std::collections::HashMap;

#[derive(Database, Clone)]
#[database("mmoldb")]
struct Db(PgPool);

struct NumFormat;

impl rocket_dyn_templates::tera::Filter for NumFormat {
    fn filter(
        &self,
        value: &Value,
        _args: &HashMap<String, Value>,
    ) -> rocket_dyn_templates::tera::Result<Value> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_i64() {
                return Ok(n.to_formatted_string(&Locale::en).into());
            }
        }

        Ok(value.clone())
    }
}

async fn run_migrations(rocket: Rocket<Build>) -> Rocket<Build> {
    use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
    let config: rocket_db_pools::Config = rocket
        .figment()
        .extract_inner("databases.mmoldb")
        .expect("mmoldb database connection information was not found in Rocket.toml");

    rocket::tokio::task::spawn_blocking(move || {
        diesel::PgConnection::establish(&config.url)
            .expect("Failed to connect to mmoldb database during migrations")
            .run_pending_migrations(MIGRATIONS)
            .expect("Failed to apply migrations");
    })
    .await
    .expect("Error joining migrations task");

    rocket
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", web::routes())
        .mount("/static", rocket::fs::FileServer::from("static"))
        .manage(IngestTask::new())
        .attach(Template::custom(|engines| {
            engines.tera.register_filter("num_format", NumFormat);
        }))
        .attach(Db::init())
        .attach(AdHoc::on_ignite("Migrations", run_migrations))
        .attach(IngestFairing::new())
}
