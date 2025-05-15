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

use log::warn;
use rocket::fairing::AdHoc;
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
        .attach(Template::fairing())
        .attach(Db::init())
        .attach(AdHoc::on_liftoff("Ingest", |rocket| {
            let pool = Db::fetch(&rocket)
                .expect("Rocket is not managing a Db pool")
                .clone();

            let is_debug = rocket.config().profile == "debug";

            Box::pin(async move {
                ingest::launch_ingest_task(pool, is_debug)
                    .await
                    .expect("TODO Figure out how to expose this as an error ")
                    .await
                    .expect("TODO What's this error")
                    .await;
            })
        }))
}
