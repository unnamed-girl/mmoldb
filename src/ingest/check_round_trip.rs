use std::fmt::format;
use itertools::{EitherOrBoth, Itertools};
use mmolb_parsing::enums::{Place, Position, Slot};
use mmolb_parsing::parsed_event::{KnownBug, PlacedPlayer};
use mmolb_parsing::ParsedEventMessage;
use strum::IntoDiscriminant;
use crate::db::RowToEventError;
use crate::ingest::EventDetail;
use crate::ingest::worker::IngestLogs;

fn log_if_error<'g, E: std::fmt::Display>(
    ingest_logs: &mut IngestLogs,
    index: usize,
    to_parsed_result: Result<ParsedEventMessage<&'g str>, E>,
    log_prefix: &str,
) -> Option<ParsedEventMessage<&'g str>> {
    match to_parsed_result {
        Ok(to_contact_result) => Some(to_contact_result),
        Err(err) => {
            ingest_logs.error(index, format!("{log_prefix}: {err}"));
            None
        }
    }
}

// When generating our events we intentionally add more detail to `Place`s than
// was originally there. This causes round-trip errors if it's not specifically
// address it. This function specifically addresses it by mutating the
// reconstructed event's places to match the original's, where they're
// compatible.
fn downgrade_parsed_places_to_match(
    game_event_index: usize,
    ours: &mut ParsedEventMessage<&str>,
    original: &ParsedEventMessage<&str>,
    ingest_logs: &mut IngestLogs,
) {
    match ours {
        ParsedEventMessage::ParseError { .. } => {}
        ParsedEventMessage::KnownBug { bug } => match bug {
            KnownBug::FirstBasemanChoosesAGhost { .. } => {}
        }
        ParsedEventMessage::LiveNow { .. } => {}
        ParsedEventMessage::PitchingMatchup { .. } => {}
        ParsedEventMessage::Lineup { .. } => {
            // There are PlacedPlayers in this event but I don't think we can upgrade them
        }
        ParsedEventMessage::PlayBall => {}
        ParsedEventMessage::GameOver { .. } => {}
        ParsedEventMessage::Recordkeeping { .. } => {}
        ParsedEventMessage::InningStart { .. } => {
            // There are PlacedPlayers in this event but I don't think we can upgrade them
        }
        ParsedEventMessage::NowBatting { .. } => {}
        ParsedEventMessage::InningEnd { .. } => {}
        ParsedEventMessage::MoundVisit { .. } => {}
        ParsedEventMessage::PitcherRemains { .. } => {}
        ParsedEventMessage::PitcherSwap { .. } => {}
        ParsedEventMessage::Ball { .. } => {}
        ParsedEventMessage::Strike { .. } => {}
        ParsedEventMessage::Foul { .. } => {}
        ParsedEventMessage::Walk { .. } => {}
        ParsedEventMessage::HitByPitch { .. } => {}
        ParsedEventMessage::FairBall { .. } => {}
        ParsedEventMessage::StrikeOut { .. } => {}
        ParsedEventMessage::BatterToBase { .. } => {}
        ParsedEventMessage::HomeRun { .. } => {}
        ParsedEventMessage::CaughtOut { .. } => {}
        ParsedEventMessage::GroundedOut { fielders, .. } => {
            if let ParsedEventMessage::GroundedOut { fielders: original_fielders, .. } = original {
                downgrade_places_to_match(game_event_index, fielders, original_fielders, ingest_logs, "GroundedOut");
            } else {
                ingest_logs.warn(game_event_index, format!(
                    "Not downgrading parsed Places because the event types don't match \
                    (reconstructed is {:?} and original is {:?})",
                    ours.discriminant(), original.discriminant(),
                ));
            }

        }
        ParsedEventMessage::ForceOut { .. } => {}
        ParsedEventMessage::ReachOnFieldersChoice { .. } => {}
        ParsedEventMessage::DoublePlayGrounded { .. } => {}
        ParsedEventMessage::DoublePlayCaught { .. } => {}
        ParsedEventMessage::ReachOnFieldingError { .. } => {}
        ParsedEventMessage::WeatherDelivery { .. } => {}
        ParsedEventMessage::WeatherDeliveryDiscard { .. } => {}
        ParsedEventMessage::WeatherShipment { .. } => {}
        ParsedEventMessage::WeatherSpecialDelivery { .. } => {}
        ParsedEventMessage::Balk { .. } => {}
    }
}

fn downgrade_places_to_match(
    game_event_index: usize,
    ours: &mut [PlacedPlayer<&str>],
    original: &[PlacedPlayer<&str>],
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    ours.into_iter()
        .zip_longest(original)
        .enumerate()
        .for_each(|(i, pair)| match pair {
            EitherOrBoth::Both(ours, original) => {
                let loc = format!("{log_loc}[{i}]");
                downgrade_place_to_match(game_event_index, ours, original, ingest_logs, &loc);
            }
            EitherOrBoth::Left(ours) => {
                ingest_logs.warn(game_event_index, format!(
                    "Not downgrading parsed Place for {} because the reconstructed event's \
                    {log_loc} item at index {i} had no corresponding item in the original",
                    ours.name,
                ));
            }
            EitherOrBoth::Right(original) => {
                ingest_logs.warn(game_event_index, format!(
                    "Not downgrading parsed Place for {} because the original event's {log_loc} \
                    item at index {i} had no corresponding item in the reconstruction",
                    original.name,
                ));
            }
        });
}

fn downgrade_place_to_match(
    game_event_index: usize,
    ours: &mut PlacedPlayer<&str>,
    original: &PlacedPlayer<&str>,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    if ours.name != original.name {
        ingest_logs.warn(game_event_index, format!(
            "Not downgrading parsed Place at {log_loc} because the reconstructed player's name \
            ({}) didn't match the original's ({})",
            ours.name, original.name,
        ));
    } else {
        match original.place {
            Place::Position(position) => {
                match position {
                    Position::Pitcher => {
                        downgrade_place_to_pitcher(game_event_index, &mut ours.place, ingest_logs, log_loc);
                    }
                    Position::StartingPitcher => {
                        downgrade_place_to_starting_pitcher(game_event_index, &mut ours.place, ingest_logs, log_loc);
                    }
                    Position::ReliefPitcher => {
                        downgrade_place_to_relief_pitcher(game_event_index, &mut ours.place, ingest_logs, log_loc);
                    }
                    _ => {
                        // Nothing to downgrade
                    }
                }
            }
            Place::Slot(_) => {
                // Not downgrading because the original is at the highest level. Also,
                // not logging because it would spam the logs.
            }
        }
    }
}

fn downgrade_place_to_pitcher(
    game_event_index: usize,
    ours: &mut Place,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    match ours {
        Place::Position(Position::Pitcher) => {
            // No action necessary
        }
        Place::Position(Position::StartingPitcher | Position::ReliefPitcher | Position::Closer) |
        Place::Slot(Slot::StartingPitcher(_) | Slot::ReliefPitcher(_) | Slot::Closer) => {
            *ours = Place::Position(Position::Pitcher)
        }
        _ => {
            ingest_logs.error(game_event_index, format!(
                "Can't \"downgrade\" {ours:?} into Position::Pitcher at {log_loc}",
            ));
        }
    }
}

fn downgrade_place_to_starting_pitcher(
    game_event_index: usize,
    ours: &mut Place,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    match ours {
        Place::Position(Position::StartingPitcher) => {
            // No action necessary
        }
        Place::Slot(Slot::StartingPitcher(_)) => {
            *ours = Place::Position(Position::Pitcher)
        }
        _ => {
            ingest_logs.error(game_event_index, format!(
                "Can't \"downgrade\" {ours:?} into Position::Pitcher at {log_loc}",
            ));
        }
    }
}

fn downgrade_place_to_relief_pitcher(
    game_event_index: usize,
    ours: &mut Place,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    match ours {
        Place::Position(Position::ReliefPitcher) => {
            // No action necessary
        }
        Place::Slot(Slot::ReliefPitcher(_)) => {
            *ours = Place::Position(Position::Pitcher)
        }
        _ => {
            ingest_logs.error(game_event_index, format!(
                "Can't \"downgrade\" {ours:?} into Position::Pitcher at {log_loc}",
            ));
        }
    }
}

// The particular combination of &str and String type arguments is
// dictated by the caller
pub fn check_round_trip(
    index: usize,
    ingest_logs: &mut IngestLogs,
    is_contact_event: bool,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &EventDetail<&str>,
    reconstructed_detail: &Result<EventDetail<String>, RowToEventError>,
) {
    let Some(mut parsed_through_detail) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    }) else {
        return;
    };

    downgrade_parsed_places_to_match(index, &mut parsed_through_detail, parsed, ingest_logs);

    if parsed != &parsed_through_detail {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through EventDetail produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\
                 Through EventDetail: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_detail,
            ),
        );
    }

    let reconstructed_detail = match reconstructed_detail {
        Ok(reconstructed_detail) => reconstructed_detail,
        Err(err) => {
            ingest_logs.error(
                index,
                format!(
                    "Attempt to round-trip {} ParsedEventMessage -> EventDetail -> database \
                    -> EventDetail -> ParsedEventMessage failed at the database -> EventDetail \
                    step with error: {err}",
                    if is_contact_event {
                        "contact event"
                    } else {
                        "event"
                    }
                ),
            );
            return;
        }
    };

    let Some(mut parsed_through_db) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            database -> EventDetail -> ParsedEventMessage failed at the EventDetail -> \
            ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> database \
            -> EventDetail -> ParsedEventMessage failed at the EventDetail -> ParsedEventMessage \
            step with error",
        )
    }) else {
        return;
    };
    
    downgrade_parsed_places_to_match(index, &mut parsed_through_db, parsed, ingest_logs);

    if parsed != &parsed_through_db {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through database produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\n\
                 Through database: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_db,
            ),
        );
    }
}