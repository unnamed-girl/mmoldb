// @generated automatically by Diesel CLI.

pub mod data {
    diesel::table! {
        data.event_baserunners (id) {
            id -> Int8,
            event_id -> Int8,
            baserunner_name -> Text,
            base_before -> Nullable<Int4>,
            base_after -> Nullable<Int4>,
            steal -> Bool,
        }
    }

    diesel::table! {
        data.event_fielders (id) {
            id -> Int8,
            event_id -> Int8,
            fielder_name -> Text,
            fielder_position -> Int8,
            play_order -> Int4,
        }
    }

    diesel::table! {
        data.events (id) {
            id -> Int8,
            ingest -> Int8,
            game_id -> Text,
            game_event_index -> Int4,
            contact_game_event_index -> Nullable<Int4>,
            inning -> Int4,
            top_of_inning -> Bool,
            event_type -> Int8,
            hit_type -> Nullable<Int8>,
            count_balls -> Int4,
            count_strikes -> Int4,
            outs_before -> Int4,
            outs_after -> Int4,
            batter_count -> Int4,
            batter_name -> Text,
            pitcher_name -> Text,
        }
    }

    diesel::table! {
        data.ingests (id) {
            id -> Int8,
            date_started -> Timestamp,
            date_finished -> Nullable<Timestamp>,
        }
    }

    diesel::joinable!(event_baserunners -> events (event_id));
    diesel::joinable!(event_fielders -> events (event_id));
    diesel::joinable!(events -> ingests (ingest));

    diesel::allow_tables_to_appear_in_same_query!(
        event_baserunners,
        event_fielders,
        events,
        ingests,
    );
}
