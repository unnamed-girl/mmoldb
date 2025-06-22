// @generated automatically by Diesel CLI.

pub mod info {
    diesel::table! {
        info.event_ingest_log (id) {
            id -> Int8,
            game_id -> Int8,
            game_event_index -> Int4,
            log_index -> Int4,
            log_level -> Int4,
            log_text -> Text,
        }
    }

    diesel::table! {
        info.game_ingest_timing (id) {
            id -> Int8,
            game_id -> Int8,
            check_already_ingested_duration -> Float8,
            parse_duration -> Float8,
            sim_duration -> Float8,
            db_insert_duration -> Float8,
            db_fetch_for_check_duration -> Float8,
            db_fetch_for_check_get_game_id_duration -> Float8,
            db_fetch_for_check_get_events_duration -> Float8,
            db_fetch_for_check_group_events_duration -> Float8,
            db_fetch_for_check_get_runners_duration -> Float8,
            db_fetch_for_check_group_runners_duration -> Float8,
            db_fetch_for_check_get_fielders_duration -> Float8,
            db_fetch_for_check_group_fielders_duration -> Float8,
            db_fetch_for_check_post_process_duration -> Float8,
            db_duration -> Float8,
            check_round_trip_duration -> Float8,
            insert_extra_logs_duration -> Float8,
            total_duration -> Float8,
        }
    }

    diesel::table! {
        info.ingests (id) {
            id -> Int8,
            started_at -> Timestamp,
            finished_at -> Nullable<Timestamp>,
            aborted_at -> Nullable<Timestamp>,
            start_next_ingest_at_page -> Nullable<Text>,
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

    diesel::allow_tables_to_appear_in_same_query!(
        event_ingest_log,
        game_ingest_timing,
        ingests,
        raw_events,
    );
}
