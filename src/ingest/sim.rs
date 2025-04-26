use mmolb_parsing::enums::{EventType, Side};
use mmolb_parsing::ParsedEvent;
use thiserror::Error;
use crate::models::NewEvent;

#[derive(Debug, Error)]
pub enum SimError {
    #[error("Expected {expected} event, but received {received}")]
    UnexpectedEventType {
        expected: EventType,
        received: EventType,
    },
}

pub struct Game {
    // This drives a state machine. I may need a new type for this if
    // the state machine states are not 1:1 with event types.
    expected_event_type: EventType,
}

impl Game {
    pub fn new() -> Game {
        Self {
            expected_event_type: EventType::LiveNow,
        }
    }

    fn check_expected_event_type(&self, received_event_type: EventType) -> Result<(), SimError> {
        // TODO Make expected_event_type a custom enum that captures this in a single state
        if self.expected_event_type == EventType::Pitch && received_event_type == EventType::NowBatting {
            Ok(())
        } else if received_event_type != self.expected_event_type {
            Err(SimError::UnexpectedEventType {
                expected: self.expected_event_type,
                received: received_event_type,
            })
        } else {
            Ok(())
        }
    }

    pub fn apply<S>(&mut self, event: &ParsedEvent<S>) -> Result<Option<NewEvent>, SimError> {
        match event {
            ParsedEvent::LiveNow => {
                self.check_expected_event_type(EventType::LiveNow)?;
                self.expected_event_type = EventType::PitchingMatchup;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::PitchingMatchup { .. } => {
                self.check_expected_event_type(EventType::PitchingMatchup)?;
                self.expected_event_type = EventType::AwayLineup;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::Lineup(Side::Away, _) => {
                self.check_expected_event_type(EventType::AwayLineup)?;
                self.expected_event_type = EventType::HomeLineup;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::Lineup(Side::Home, _) => {
                self.check_expected_event_type(EventType::HomeLineup)?;
                self.expected_event_type = EventType::PlayBall;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::PlayBall => {
                self.check_expected_event_type(EventType::PlayBall)?;
                self.expected_event_type = EventType::InningStart;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::InningStart { .. } => {
                self.check_expected_event_type(EventType::InningStart)?;
                self.expected_event_type = EventType::NowBatting;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::NowBatting { .. } => {
                self.check_expected_event_type(EventType::NowBatting)?;
                self.expected_event_type = EventType::Pitch;
                // Procedural event, not recorded
                Ok(None)
            }
            ParsedEvent::Ball => {
                self.check_expected_event_type(EventType::Pitch)?;
                // Next expected event type is also Pitch
                Ok(Some(self.make_ball()))
            }
            ParsedEvent::Strike { .. } => {
                self.check_expected_event_type(EventType::Pitch)?;
                // Next expected event type is also Pitch
                Ok(Some(RecordableEvent::Strike))
            }
            ParsedEvent::Foul { .. } => {
                self.check_expected_event_type(EventType::Pitch)?;
                // Next expected event type is also Pitch
                Ok(Some(RecordableEvent::Foul))
            }
            ParsedEvent::Hit { .. } => {
                self.check_expected_event_type(EventType::Pitch)?;
                // Next expected event type is also Pitch
                Ok(Some(RecordableEvent::Hit))
            }
            ParsedEvent::Out { .. } => {
                self.check_expected_event_type(EventType::Pitch)?;
                // Next expected event type is also Pitch
                Ok(Some(RecordableEvent::Out))
            }
            ParsedEvent::MoundVisit { .. } => { todo!() }
            ParsedEvent::MoundVisitRefused => { todo!() }
            ParsedEvent::PitcherSwap { .. } => { todo!() }
            ParsedEvent::GameOver => { todo!() }
            ParsedEvent::RunnerAdvance { .. } => { todo!() }
            ParsedEvent::Error { .. } => { todo!() }
            ParsedEvent::Recordkeeping { .. } => { todo!() }
            ParsedEvent::Pitch(_) => { todo!() }
            ParsedEvent::Walk => { todo!() }
            ParsedEvent::BatterToBase { .. } => { todo!() }
            ParsedEvent::Scores { .. } => { todo!() }
            ParsedEvent::HitByPitch => { todo!() }
            ParsedEvent::InningEnd { .. } => { todo!() }
            ParsedEvent::ParseError { .. } => { todo!() }
        }
    }
}