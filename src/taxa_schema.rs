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
        taxa.fielder_location (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            abbreviation -> Text,
            area -> Text,
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
        taxa.leagues (id) {
            id -> Int8,
            name -> Text,
            mmolb_league_id -> Text,
            parent_team_id -> Text,
            emoji -> Text,
            color -> Text,
            league_type -> Text,
        }
    }

    diesel::table! {
        taxa.slot (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            abbreviation -> Text,
            role -> Text,
            pitcher_type -> Nullable<Text>,
            slot_number -> Nullable<Int4>,
            location -> Nullable<Int8>,
        }
    }

    diesel::joinable!(slot -> fielder_location (location));

    diesel::allow_tables_to_appear_in_same_query!(
        base,
        base_description_format,
        event_type,
        fair_ball_type,
        fielder_location,
        fielding_error_type,
        hit_type,
        pitch_type,
        slot,
    );
}
