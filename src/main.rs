#[warn(clippy::future_not_send)]

mod db;
#[rustfmt::skip] // This is a generated file
mod data_schema;
#[rustfmt::skip] // This is a generated file
mod taxa_schema;
mod ingest;
mod models;

use chrono::{TimeZone, Utc};
use chrono_humanize::HumanTime;
use log::error;
use rocket::fairing::AdHoc;
use rocket::http::Status;
use rocket::log::private::warn;
use rocket::response::Responder;
use rocket::{Request, Response, get, launch, routes};
use rocket_db_pools::{Connection, Database, diesel::PgPool};
use rocket_dyn_templates::{Template, context};
use serde::Serialize;
use thiserror::Error;

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
        let rendered = Template::show(
            req.rocket(),
            "error",
            context! {
                error_text: format!("{:#?}", self),
            },
        )
        .unwrap();

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
        age: String,
    }

    let ingests: Vec<_> = db::latest_ingests(&mut db)
        .await?
        .into_iter()
        .map(|ingest| IngestContext {
            age: HumanTime::from(Utc.from_utc_datetime(&ingest.date_started)).to_text_en(
                chrono_humanize::Accuracy::Precise,
                chrono_humanize::Tense::Past,
            ),
        })
        .collect();

    Ok(Template::render(
        "index",
        context! {
            ingests: ingests,
        },
    ))
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

            Box::pin(async {
                ingest::launch_ingest_task(pool)
                    .await
                    .expect("TODO Figure out how to expose this as an error ")
                    .await
                    .expect("TODO What's this error")
                    .await;
            })
        }))
}
