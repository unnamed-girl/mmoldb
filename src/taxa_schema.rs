// @generated automatically by Diesel CLI.

pub mod taxa {
    diesel::table! {
        taxa.event_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }
}
