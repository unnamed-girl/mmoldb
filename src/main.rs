#[warn(clippy::future_not_send)]

mod db;
#[rustfmt::skip] // This is a generated file
mod data_schema;
#[rustfmt::skip] // This is a generated file
mod taxa_schema;
mod ingest;
mod models;

use chrono::{NaiveDateTime, TimeZone, Utc};
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
    struct FormattedDateContext {
        relative: String,
        absolute: String,
    }

    impl From<&NaiveDateTime> for FormattedDateContext {
        fn from(value: &NaiveDateTime) -> Self {
            let value_utc = Utc.from_utc_datetime(value);
            let human = HumanTime::from(value_utc);
            
            Self {
                relative: human.to_string(),
                absolute: value_utc.to_string(),
            }
        }
    }

    #[derive(Serialize)]
    struct IngestContext {
        id: i64,
        started_at: FormattedDateContext,
        finished_at: Option<FormattedDateContext>,
    }

    let ingests: Vec<_> = db::latest_ingests(&mut db)
        .await?
        .into_iter()
        .map(|ingest| IngestContext {
            id: ingest.id,
            started_at: (&ingest.date_started).into(),
            finished_at: ingest.date_finished.as_ref().map(Into::into),
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
