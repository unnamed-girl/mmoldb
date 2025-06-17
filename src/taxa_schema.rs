// @generated automatically by Diesel CLI.

pub mod taxa {
    diesel::table! {
        taxa.base (id) {
            id -> Int8,
            name -> Text,
            bases_achieved -> Int8,
        }
    }

    diesel::table! {
        taxa.base_description_format (id) {
            id -> Int8,
            name -> Text,
        }
    }

    diesel::table! {
        taxa.event_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            ends_pa -> Bool,
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
        taxa.fielding_error_type (id) {
            id -> Int8,
            name -> Text,
        }
    }

    diesel::table! {
        taxa.hit_type (id) {
            id -> Int8,
            name -> Text,
            base_number -> Int8,
        }
    }

    diesel::table! {
        taxa.pitch_type (id) {
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
            abbreviation -> Text,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(
        base,
        base_description_format,
        event_type,
        fair_ball_type,
        fielding_error_type,
        hit_type,
        pitch_type,
        position,
    );
}
