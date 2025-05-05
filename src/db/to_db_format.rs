use mmolb_parsing::parsed_event::PositionedPlayer;
use crate::db::taxa::{Taxa};
use crate::ingest::EventDetail;
use crate::models::{DbEvent, DbFielder, NewEvent, NewFielder};

pub fn event_to_row<'e>(
    taxa: &Taxa,
    ingest_id: i64,
    event: &'e EventDetail<&'e str>,
) -> NewEvent<'e> {
    NewEvent {
        ingest: ingest_id,
        game_id: event.game_id,
        game_event_index: event.game_event_index as i32,
        contact_game_event_index: event.contact_game_event_index.map(|i| i as i32),
        inning: event.inning as i32,
        top_of_inning: event.top_of_inning,
        event_type: taxa.event_type_id(event.detail_type),
        // TODO Figure out why this is violating foreign key constraints
        hit_type: None, // event.hit_type.map(|ty| taxa.hit_type_id(ty)),
        count_balls: event.count_balls as i32,
        count_strikes: event.count_strikes as i32,
        outs_before: event.outs_before,
        outs_after: event.outs_after,
        batter_count: event.batter_count as i32,
        batter_name: event.batter_name,
        pitcher_name: event.pitcher_name,
    }
}

pub fn event_to_fielders<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewFielder<'e>> {
    event.fielders.iter()
        .enumerate()
        .map(|(i, fielder)| NewFielder {
            event_id,
            fielder_name: fielder.name,
            fielder_position: taxa.position_id(fielder.position.into()),
            play_order: i as i32,
        })
        .collect()
}

pub fn row_to_event<'e>(
    taxa: &Taxa,
    row: DbEvent,
    fielders: Vec<DbFielder>,
) -> EventDetail<String> {
    let fielders = fielders
        .into_iter()
        .map(|f| {
            PositionedPlayer {
                name: f.fielder_name,
                position: taxa.position_from_id(f.fielder_position).into(),
            }
        })
        .collect();
    
    EventDetail {
        game_id: row.game_id,
        game_event_index: row.game_event_index as usize,
        contact_game_event_index: row.contact_game_event_index.map(|i| i as usize),
        inning: row.inning as u8,
        top_of_inning: row.top_of_inning,
        count_balls: row.count_balls as u8,
        count_strikes: row.count_strikes as u8,
        outs_before: row.outs_before,
        outs_after: row.outs_after,
        batter_count: row.batter_count as usize,
        batter_name: row.batter_name,
        pitcher_name: row.pitcher_name,
        detail_type: taxa.event_type_from_id(row.event_type),
        hit_type: row.hit_type.map(|ty| taxa.hit_type_from_id(ty)),
        advances: Vec::new(), // TODO
        fielders,
    }
}
