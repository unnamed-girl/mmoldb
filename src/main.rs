mod db;
mod models;

use rocket::{get, routes, launch, Rocket, Build};
use rocket::log::private::warn;
use rocket_dyn_templates::{context, Template};
use rocket_db_pools::{Database, Connection};
use rocket_db_pools::diesel::{QueryResult, PgPool, prelude::*};

#[derive(Database)]
#[database("mmoldb")]
struct Db(PgPool);

#[get("/")]
fn index() -> Template {
    let ingests = db::latest_ingests();

    Template::render("index", context! {
        ingests: ingests,
    })
}

#[launch]
fn rocket() -> _ {
    warn!("TODO: Apply pending Diesel migrations");
    
    rocket::build()
        .mount("/", routes![index])
        .mount("/static", rocket::fs::FileServer::from("static"))
        .attach(Template::fairing())
        .attach(Db::init())
}