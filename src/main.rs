use rocket::{get, routes, launch};
use rocket_dyn_templates::{context, Template};

#[get("/")]
fn index() -> Template {
    Template::render("index", context! {})
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![index])
        .mount("/static", rocket::fs::FileServer::from("static"))
        .attach(Template::fairing())
}