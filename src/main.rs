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
use log::warn;
use rocket::launch;
use rocket_db_pools::{Database, diesel::PgPool};
use rocket_dyn_templates::Template;

#[derive(Database, Clone)]
#[database("mmoldb")]
struct Db(PgPool);

#[launch]
fn rocket() -> _ {
    warn!("TODO: Apply pending Diesel migrations");

    rocket::build()
        .mount("/", web::routes())
        .mount("/static", rocket::fs::FileServer::from("static"))
        .manage(IngestTask::new())
        .attach(Template::fairing())
        .attach(Db::init())
        .attach(IngestFairing::new())
}
