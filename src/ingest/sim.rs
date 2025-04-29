use log::warn;
use mmolb_parsing::enums::{EventType, Side, Position};
use mmolb_parsing::ParsedEvent;
use mmolb_parsing::parsing::ParsedEventDiscriminants;
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimError {
    #[error("This game had no events")]
    NoEvents,
    
    #[error("Not enough events. Expected {expected:?} event after {previous:?}")]
    NotEnoughEvents {
        expected: ParsedEventDiscriminants,
        previous: ParsedEventDiscriminants,
    },
    
    #[error("Expected {expected:?} event after {previous:?}, but received {received:?}")]
    UnexpectedEventType {
        expected: ParsedEventDiscriminants,
        previous: Option<ParsedEventDiscriminants>,
        received: ParsedEventDiscriminants,
    },
}

#[derive(Debug)]
pub struct EventDetail<'a> {
    pub game_id: &'a str,
    pub game_event_index: i32,
    pub inning: i32,
    pub top_of_inning: bool,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub ends_inning: bool,
    pub batter_count: i32,
    pub batter_name: &'a str,
    pub pitcher_name: &'a str,
    pub fielder_names: Vec<&'a str>,

    pub type_detail: EventTypeDetail,
}

#[derive(Debug)]
pub enum EventTypeDetail {
    Ball,
    Strike,
    Foul,
    Hit,
    Out,
}

enum GamePhase {
    ExpectInningStart,
    ExpectBatterUp,
    ExpectPitch,
}

pub struct Game<'g, IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)>> {
    // Should never change
    events: ParsedEventIter<'g, IterT>,
    game_id: &'g str,
    
    // Should not change most of the time, but may change occasionally 
    // due to augments
    away_pitcher_name: &'g str,
    home_pitcher_name: &'g str,
    away_lineup: Vec<(Position, &'g str)>,
    home_lineup: Vec<(Position, &'g str)>,
    
    // Change all the time
    prev_event_type: ParsedEventDiscriminants,
    phase: GamePhase,
    inning_number: i32,
    inning_batting_side: Side,
    count_balls: i32,
    count_strikes: i32,
    outs: i32,
    batter_count: Option<usize>,
    batter_name: Option<&'g str>,
}

struct ParsedEventIter<'game, IterT: Iterator<Item=(usize, ParsedEvent<&'game str>)>> {
    inner: IterT,
    prev_event_type: Option<ParsedEventDiscriminants>,
}

impl<'game, IterT: Iterator<Item=(usize, ParsedEvent<&'game str>)>> ParsedEventIter<'game, IterT> {
    pub fn new(iter: IterT) -> Self { 
        Self {
            prev_event_type: None,
            inner: iter,
        } 
    }
    
    pub fn next(&mut self, expected: ParsedEventDiscriminants) -> Result<(usize, ParsedEvent<&'game str>), SimError> {
        match self.inner.next() {
            Some((i, val)) => {
                self.prev_event_type = Some(val.discriminant());
                Ok((i, val))
            },
            None => match self.prev_event_type {
                None => Err(SimError::NoEvents),
                Some(previous) => Err(SimError::NotEnoughEvents {
                    expected,
                    previous,
                }),
            }
        }
    }
}

macro_rules! extract_next {
    ($iter:expr, $expected:expr, $($p:pat => $e:expr),+) => {
        match $iter.next($expected)? {
            $($p => Ok($e),)*
            (_, other) => Err(SimError::UnexpectedEventType {
                expected: $expected,
                previous: $iter.prev_event_type,
                received: other.discriminant(),
            })
        }
    };
    ($iter:expr, $disc:expr, $($p:pat => $e:expr,)+) => {
        match $iter.next($disc)? {
            $($p => Ok($e),)*
            (_, other) => Err(SimError::UnexpectedEventType {
                expected: $disc,
                previous: $iter.prev_event_type,
                received: other.discriminant(),
            })
        }
    };
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::Away => { Side::Home }
        Side::Home => { Side::Away }
    }
}

impl<'g, IterT> Game<'g, IterT>
where IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)> {
    // TODO Figure out how to accept a simple iterator of ParsedEvent and
    //   do the enumerate myself
    pub fn new(game_id: &'g str, events: IterT) -> Result<Game<'g, IterT>, SimError> {
        let mut events = ParsedEventIter::new(events);
        
        extract_next!(
            events,
            ParsedEventDiscriminants::LiveNow,
            (_, ParsedEvent::LiveNow) => ()
        )?;
        
        let (home_pitcher_name, away_pitcher_name) = extract_next!(
            events,
            ParsedEventDiscriminants::PitchingMatchup,
            (_, ParsedEvent::PitchingMatchup { home_pitcher, away_pitcher}) => (home_pitcher, away_pitcher)
        )?;
        
        let away_lineup = extract_next!(
            events,
            ParsedEventDiscriminants::Lineup,
            (_, ParsedEvent::Lineup { side: Side::Away, players }) => players
        )?;
        
        let home_lineup = extract_next!(
            events,
            ParsedEventDiscriminants::Lineup,
            (_, ParsedEvent::Lineup { side: Side::Home, players }) => players
        )?;

        extract_next!(
            events,
            ParsedEventDiscriminants::PlayBall,
            (_, ParsedEvent::PlayBall) => ()
        )?;

        Ok(Self {
            events,
            game_id,
            away_pitcher_name,
            home_pitcher_name,
            away_lineup,
            home_lineup,
            prev_event_type: ParsedEventDiscriminants::PlayBall,
            phase: GamePhase::ExpectInningStart,
            inning_number: 0,
            inning_batting_side: Side::Home,
            count_balls: 0,
            count_strikes: 0,
            outs: 0,
            batter_count: None,
            batter_name: None,
        })
    }

    fn active_lineup(&self) -> &[(Position, &'g str)] {
        match self.inning_batting_side {
            Side::Away => &self.away_lineup,
            Side::Home => &self.home_lineup,
        }
    }

    pub fn next(&mut self) -> Result<Option<EventDetail>, SimError> {
        match self.phase {
            GamePhase::ExpectInningStart => {
                let (number, side, _batting_team, _pitcher) = extract_next!(
                    self.events,
                    ParsedEventDiscriminants::InningStart,
                    (_, ParsedEvent::InningStart { number, side, batting_team, pitcher }) => (number as i32, side, batting_team, pitcher),
                )?;
                
                if number != self.inning_number + 1 {
                    warn!("Unexpected inning number in {}: expected {}, but saw {number}", self.game_id, self.inning_number + 1)
                }
                self.inning_number = number;
                
                if side != opposite_side(self.inning_batting_side) {
                    warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, opposite_side(self.inning_batting_side))
                }
                self.inning_batting_side = side;
                
                // TODO Make use of batting_team and pitcher

                self.phase = GamePhase::ExpectBatterUp;
                Ok(None)
            }
            GamePhase::ExpectBatterUp => {
                let (batter_name, first_pa) = extract_next!(
                    self.events,
                    ParsedEventDiscriminants::NowBatting,
                    (_, ParsedEvent::NowBatting { batter, first_pa }) => (batter, first_pa),
                )?;

                self.batter_name = Some(batter_name);
                let batter_count = if let Some(prev_batter_count) = self.batter_count {
                    self.batter_count = Some(prev_batter_count + 1);
                    prev_batter_count + 1
                } else {
                    self.batter_count = Some(0);
                    0
                };

                let lineup = self.active_lineup();
                let predicted_batter_name = lineup[batter_count % lineup.len()].1;
                if batter_name != predicted_batter_name {
                    warn!("Unexpected batter up in {}: expected {predicted_batter_name}, but saw {batter_name}", self.game_id);
                }

                self.phase = GamePhase::ExpectPitch;
                Ok(None)
            }
            GamePhase::ExpectPitch => {
                let () = extract_next!(
                    self.events,
                    ParsedEventDiscriminants::Pitch,
                    (_, ParsedEvent::Pitch { pitch }) => (),
                )?;

                Ok(todo!())
            }
        }
        // 
        // 
        // Ok(match event {
        //     ParsedEvent::LiveNow => {
        //         self.check_expected_event_type(EventType::LiveNow)?;
        //         self.expected_event_type = EventType::PitchingMatchup;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::PitchingMatchup { away_pitcher, home_pitcher } => {
        //         self.check_expected_event_type(EventType::PitchingMatchup)?;
        //         self.expected_event_type = EventType::AwayLineup;
        // 
        //         self.away_pitcher_name = Some(away_pitcher);
        //         self.home_pitcher_name = Some(home_pitcher);
        // 
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::Lineup(Side::Away, _) => {
        //         self.check_expected_event_type(EventType::AwayLineup)?;
        //         self.expected_event_type = EventType::HomeLineup;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::Lineup(Side::Home, _) => {
        //         self.check_expected_event_type(EventType::HomeLineup)?;
        //         self.expected_event_type = EventType::PlayBall;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::PlayBall => {
        //         self.check_expected_event_type(EventType::PlayBall)?;
        //         self.expected_event_type = EventType::InningStart;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::InningStart { .. } => {
        //         self.check_expected_event_type(EventType::InningStart)?;
        //         self.expected_event_type = EventType::NowBatting;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::NowBatting { .. } => {
        //         self.check_expected_event_type(EventType::NowBatting)?;
        //         self.expected_event_type = EventType::Pitch;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::Ball => {
        //         self.check_expected_event_type(EventType::Pitch)?;
        //         // Next expected event type is also Pitch
        //         Ok(Some(EventTypeDetail::Ball))
        //     }
        //     ParsedEvent::Strike { .. } => {
        //         self.check_expected_event_type(EventType::Pitch)?;
        //         // Next expected event type is also Pitch
        //         Ok(Some(EventTypeDetail::Strike))
        //     }
        //     ParsedEvent::Foul { .. } => {
        //         self.check_expected_event_type(EventType::Pitch)?;
        //         // Next expected event type is also Pitch
        //         Ok(Some(EventTypeDetail::Foul))
        //     }
        //     ParsedEvent::Hit { .. } => {
        //         self.check_expected_event_type(EventType::Pitch)?;
        //         // Next expected event type is also Pitch
        //         Ok(Some(EventTypeDetail::Hit))
        //     }
        //     ParsedEvent::Out { .. } => {
        //         self.check_expected_event_type(EventType::Pitch)?;
        //         // Next expected event type is also Pitch
        //         Ok(Some(EventTypeDetail::Out))
        //     }
        //     ParsedEvent::InningEnd { .. } => {
        //         self.check_expected_event_type(EventType::InningEnd)?;
        //         self.expected_event_type = EventType::InningStart;
        //         // Procedural event, not recorded
        //         Ok(None)
        //     }
        //     ParsedEvent::MoundVisit { .. } => { todo!() }
        //     ParsedEvent::MoundVisitRefused => { todo!() }
        //     ParsedEvent::PitcherSwap { .. } => { todo!() }
        //     ParsedEvent::GameOver => { todo!() }
        //     ParsedEvent::RunnerAdvance { .. } => { todo!() }
        //     ParsedEvent::Error { .. } => { todo!() }
        //     ParsedEvent::Recordkeeping { .. } => { todo!() }
        //     ParsedEvent::Pitch(_) => { todo!() }
        //     ParsedEvent::Walk => { todo!() }
        //     ParsedEvent::BatterToBase { .. } => { todo!() }
        //     ParsedEvent::Scores { .. } => { todo!() }
        //     ParsedEvent::HitByPitch => { todo!() }
        //     ParsedEvent::ParseError { .. } => { todo!() }
        // }?
        //     .map(|type_detail| EventDetail {
        //         game_id: &self.game_id,
        //         game_event_index,
        //         inning: self.inning,
        //         top_of_inning: self.top_of_inning,
        //         count_balls: self.count_balls,
        //         count_strikes: self.count_strikes,
        //         outs_before: self.outs,
        //         outs_after: self.outs, // TODO Caught stealing adds outs
        //         ends_inning: false, // TODO
        //         batter_count: self.batter_count,
        //         batter_name: self.batter_name,
        //         pitcher_name: self.pitcher_name,
        //         fielder_names: vec![],
        //         type_detail,
        //     })
        // )
    }
}