use log::error;
use rocket::http::Status;
use rocket::response::Responder;
use rocket::{uri, Request, Response};
use rocket_dyn_templates::{Template, context};
use thiserror::Error;

use crate::web::pages::rocket_uri_macro_index_page;

#[derive(Debug, Error)]
pub enum AppError {
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
                index_url: uri!(index_page()),
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
