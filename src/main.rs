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

use std::collections::HashMap;
use crate::ingest::{IngestFairing, IngestTask};
use log::warn;
use rocket::launch;
use rocket_db_pools::{Database, diesel::PgPool};
use rocket_dyn_templates::Template;
use rocket_dyn_templates::tera::Value;
use num_format::{Locale, ToFormattedString};

#[derive(Database, Clone)]
#[database("mmoldb")]
struct Db(PgPool);


struct NumFormat;

impl rocket_dyn_templates::tera::Filter for NumFormat {
    fn filter(&self, value: &Value, _args: &HashMap<String, Value>) -> rocket_dyn_templates::tera::Result<Value> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_i64() {
                return Ok(n.to_formatted_string(&Locale::en).into())
            }
        }

        Ok(value.clone())
    }
}

#[launch]
fn rocket() -> _ {
    warn!("TODO: Apply pending Diesel migrations");

    rocket::build()
        .mount("/", web::routes())
        .mount("/static", rocket::fs::FileServer::from("static"))
        .manage(IngestTask::new())
        .attach(Template::custom(|engines| {
            engines.tera.register_filter("num_format", NumFormat);
        }))
        .attach(Db::init())
        .attach(IngestFairing::new())
}
