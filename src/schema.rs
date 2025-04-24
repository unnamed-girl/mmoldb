// @generated automatically by Diesel CLI.

diesel::table! {
    ingests (id) {
        id -> Int8,
        date_started -> Timestamp,
        date_finished -> Nullable<Timestamp>,
    }
}
