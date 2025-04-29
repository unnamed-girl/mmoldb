use crate::db::taxa::{Taxa, TaxaEventType};
use crate::ingest::{EventDetail, EventTypeDetail};
use crate::models::{NewBaserunner, NewEvent};

pub fn event_to_row<'e>(taxa: &Taxa, ingest_id: i64, event: &'e EventDetail<'e>) -> (NewEvent<'e>, Vec<NewBaserunner<'e>>) {
    match event.type_detail {
        EventTypeDetail::Ball => {
            basic_event(taxa, ingest_id, event, TaxaEventType::Ball)
        }
        EventTypeDetail::Strike => {
            basic_event(taxa, ingest_id, event, TaxaEventType::Strike)
        }
        EventTypeDetail::Foul => {
            basic_event(taxa, ingest_id, event, TaxaEventType::Foul)
        }
        EventTypeDetail::Hit => {
            basic_event(taxa, ingest_id, event, TaxaEventType::Hit)
        }
        EventTypeDetail::Out => {
            basic_event(taxa, ingest_id, event, TaxaEventType::Out)
        }
    }
}

fn basic_event<'e>(taxa: &Taxa, ingest_id: i64, event: &'e EventDetail<'e>, event_type: TaxaEventType) -> (NewEvent<'e>, Vec<NewBaserunner<'e>>) {
    let new_event = NewEvent {
        ingest: ingest_id,
        game_id: event.game_id,
        game_event_index: event.game_event_index,
        inning: event.inning,
        top_of_inning: event.top_of_inning,
        event_type: taxa.event_type(event_type),
        count_balls: event.count_balls,
        count_strikes: event.count_strikes,
        outs_before: event.outs_before,
        outs_after: event.outs_after,
        ends_inning: event.ends_inning,
        batter_count: event.batter_count,
        batter_name: event.batter_name,
        pitcher_name: event.pitcher_name,
        fielder_names: Vec::new(),
    };

    (new_event, vec![])
}