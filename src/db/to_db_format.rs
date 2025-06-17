use crate::db::taxa::Taxa;
use crate::ingest::{EventDetail, EventDetailFielder, EventDetailRunner};
use crate::models::{DbEvent, DbFielder, DbRunner, NewBaserunner, NewEvent, NewFielder};
use thiserror::Error;

pub fn event_to_row<'e>(
    taxa: &Taxa,
    game_id: i64,
    event: &'e EventDetail<&'e str>,
) -> NewEvent<'e> {
    NewEvent {
        game_id,
        game_event_index: event.game_event_index as i32,
        fair_ball_event_index: event.fair_ball_event_index.map(|i| i as i32),
        inning: event.inning as i32,
        top_of_inning: event.top_of_inning,
        event_type: taxa.event_type_id(event.detail_type),
        hit_type: event.hit_type.map(|ty| taxa.hit_type_id(ty)),
        fair_ball_type: event.fair_ball_type.map(|ty| taxa.fair_ball_type_id(ty)),
        fair_ball_direction: event.fair_ball_direction.map(|ty| taxa.position_id(ty)),
        fielding_error_type: event
            .fielding_error_type
            .map(|ty| taxa.fielding_error_type_id(ty)),
        pitch_type: event.pitch_type.map(|ty| taxa.pitch_type_id(ty)),
        pitch_speed: event.pitch_speed,
        pitch_zone: event.pitch_zone,
        described_as_sacrifice: event.described_as_sacrifice,
        count_balls: event.count_balls as i32,
        count_strikes: event.count_strikes as i32,
        outs_before: event.outs_before,
        outs_after: event.outs_after,
        batter_name: event.batter_name,
        pitcher_name: event.pitcher_name,
    }
}

pub fn event_to_baserunners<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewBaserunner<'e>> {
    event
        .baserunners
        .iter()
        .map(|runner| NewBaserunner {
            event_id,
            baserunner_name: runner.name,
            base_before: runner.base_before.map(|b| taxa.base_id(b)),
            base_after: taxa.base_id(runner.base_after),
            is_out: runner.is_out,
            base_description_format: runner
                .base_description_format
                .map(|f| taxa.base_description_format_id(f)),
            steal: runner.is_steal,
        })
        .collect()
}

pub fn event_to_fielders<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewFielder<'e>> {
    event
        .fielders
        .iter()
        .enumerate()
        .map(|(i, fielder)| NewFielder {
            event_id,
            fielder_name: fielder.name,
            fielder_position: taxa.position_id(fielder.position),
            play_order: i as i32,
            perfect_catch: fielder.is_perfect_catch,
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum RowToEventError {
    #[error("Database returned invalid event type id {0}")]
    InvalidEventTypeId(i64),
}

pub fn row_to_event<'e>(
    taxa: &Taxa,
    event: DbEvent,
    runners: Vec<DbRunner>,
    fielders: Vec<DbFielder>,
) -> Result<EventDetail<String>, RowToEventError> {
    let baserunners = runners
        .into_iter()
        .map(|r| EventDetailRunner {
            name: r.baserunner_name,
            base_before: r.base_before.map(|id| taxa.base_from_id(id)),
            base_after: taxa.base_from_id(r.base_after),
            is_out: r.is_out,
            base_description_format: r
                .base_description_format
                .map(|id| taxa.base_description_format_from_id(id)),
            is_steal: r.steal,
        })
        .collect();

    let fielders = fielders
        .into_iter()
        .map(|f| EventDetailFielder {
            name: f.fielder_name,
            position: taxa.position_from_id(f.fielder_position).into(),
            is_perfect_catch: f.perfect_catch,
        })
        .collect();

    Ok(EventDetail {
        game_event_index: event.game_event_index as usize,
        fair_ball_event_index: event.fair_ball_event_index.map(|i| i as usize),
        inning: event.inning as u8,
        top_of_inning: event.top_of_inning,
        count_balls: event.count_balls as u8,
        count_strikes: event.count_strikes as u8,
        outs_before: event.outs_before,
        outs_after: event.outs_after,
        batter_name: event.batter_name,
        pitcher_name: event.pitcher_name,
        detail_type: taxa
            .event_type_from_id(event.event_type)
            .ok_or_else(|| RowToEventError::InvalidEventTypeId(event.event_type))?,
        hit_type: event.hit_type.map(|id| taxa.hit_type_from_id(id)),
        fair_ball_type: event
            .fair_ball_type
            .map(|id| taxa.fair_ball_type_from_id(id)),
        fair_ball_direction: event
            .fair_ball_direction
            .map(|id| taxa.position_from_id(id)),
        fielding_error_type: event
            .fielding_error_type
            .map(|id| taxa.fielding_error_type_from_id(id)),
        pitch_type: event.pitch_type.map(|id| taxa.pitch_type_from_id(id)),
        pitch_speed: event.pitch_speed,
        pitch_zone: event.pitch_zone,
        described_as_sacrifice: event.described_as_sacrifice,
        fielders,
        baserunners,
    })
}
