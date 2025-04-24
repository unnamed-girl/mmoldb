mod db;
mod schema;
mod models;
mod ingest;

use log::error;
use rocket::{get, launch, routes, Request, Response};
use rocket::fairing::AdHoc;
use rocket::http::Status;
use rocket::log::private::warn;
use rocket::response::Responder;
use rocket_dyn_templates::{context, Template};
use rocket_db_pools::{diesel::PgPool, Connection, Database};
use thiserror::Error;
use serde::Serialize;

#[derive(Database, Clone)]
#[database("mmoldb")]
struct Db(PgPool);

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    DbError(#[from] diesel::result::Error),
}

impl<'r, 'o: 'r> Responder<'r, 'o> for AppError {
    fn respond_to(self, req: &'r Request<'_>) -> rocket::response::Result<'o> {
        error!("{:#?}", self);

        // TODO Figure out how to properly turn a thiserror enum into a Template
        let rendered = Template::show(req.rocket(), "error", context! {
            error_text: format!("{:#?}", self),
        }).unwrap();

        Response::build()
            .status(Status::InternalServerError)
            .header(rocket::http::ContentType::HTML)
            .sized_body(rendered.len(), std::io::Cursor::new(rendered))
            .ok()
    }
}

#[get("/")]
async fn index(mut db: Connection<Db>) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct IngestContext {

    }

    let ingests: Vec<_> = db::latest_ingests(&mut db).await?.into_iter()
        .map(|ingest| IngestContext {})
        .collect();

    Ok(Template::render("index", context! {
        ingests: ingests,
    }))
}

#[launch]
fn rocket() -> _ {
    warn!("TODO: Apply pending Diesel migrations");

    rocket::build()
        .mount("/", routes![index])
        .mount("/static", rocket::fs::FileServer::from("static"))
        .attach(Template::fairing())
        .attach(Db::init())
        .attach(AdHoc::on_liftoff("Ingest", |rocket| {
            let pool = Db::fetch(&rocket)
                .expect("Rocket is not managing a Db pool")
                .clone();
            
            rocket::tokio::spawn(async move { ingest::ingest_task(pool).await });
            
            Box::pin(async {})
        }))
}