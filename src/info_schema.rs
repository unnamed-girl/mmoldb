// @generated automatically by Diesel CLI.

pub mod info {
    diesel::table! {
        info.event_ingest_log (id) {
            id -> Int8,
            raw_event_id -> Int8,
            log_order -> Int4,
            log_level -> Int4,
            log_text -> Text,
        }
    }

    diesel::table! {
        info.raw_events (id) {
            id -> Int8,
            game_id -> Int8,
            game_event_index -> Int4,
            event_text -> Text,
        }
    }

    diesel::joinable!(event_ingest_log -> raw_events (raw_event_id));

    diesel::allow_tables_to_appear_in_same_query!(
        event_ingest_log,
        raw_events,
    );
}
