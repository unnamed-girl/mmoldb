// @generated automatically by Diesel CLI.

pub mod taxa {
    diesel::table! {
        taxa.event_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }

    diesel::table! {
        taxa.fair_ball_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }

    diesel::table! {
        taxa.hit_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }

    diesel::table! {
        taxa.position (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(
        event_type,
        fair_ball_type,
        hit_type,
        position,
    );
}
