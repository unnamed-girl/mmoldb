// @generated automatically by Diesel CLI.

pub mod data {
    diesel::table! {
        data.events (id) {
            id -> Int8,
            ingest -> Int8,
            game_id -> Text,
            game_event_index -> Int4,
            inning -> Int4,
            top_of_inning -> Bool,
            event_type -> Int8,
            count_balls -> Int4,
            count_strikes -> Int4,
            outs_before -> Int4,
            outs_after -> Int4,
            ends_inning -> Bool,
            batter_count -> Int4,
            batter_name -> Text,
            pitcher_name -> Text,
            fielder_names -> Array<Nullable<Text>>,
        }
    }

    diesel::table! {
        data.ingests (id) {
            id -> Int8,
            date_started -> Timestamp,
            date_finished -> Nullable<Timestamp>,
        }
    }

    diesel::joinable!(events -> ingests (ingest));

    diesel::allow_tables_to_appear_in_same_query!(
        events,
        ingests,
    );
}
