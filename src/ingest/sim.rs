use log::warn;
use mmolb_parsing::enums::{FoulType, HomeAway, Position, StrikeType, TopBottom};
use mmolb_parsing::ParsedEvent;
use mmolb_parsing::parsed_event::{ParsedEventDiscriminants, PositionedPlayer, RunnerAdvance};
use strum::IntoDiscriminant;
use thiserror::Error;
use crate::db::TaxaEventType;

#[derive(Debug, Error)]
pub enum SimError {
    #[error("This game had no events")]
    NoEvents,
    
    #[error("Not enough events. Expected {expected:?} event after {previous:?}")]
    NotEnoughEvents {
        expected: &'static [ParsedEventDiscriminants],
        previous: ParsedEventDiscriminants,
    },
    
    #[error("Expected {expected:?} event after {previous:?}, but received {received:?}")]
    UnexpectedEventType {
        expected: &'static [ParsedEventDiscriminants],
        previous: Option<ParsedEventDiscriminants>,
        received: ParsedEventDiscriminants,
    },
}

#[derive(Debug)]
pub struct EventDetail<'a> {
    pub game_id: &'a str,
    pub game_event_index: usize,
    pub contact_game_event_index: Option<usize>,
    pub inning: u8,
    pub top_of_inning: bool,
    pub count_balls: u8,
    pub count_strikes: u8,
    pub outs_before: i32,
    pub outs_after: i32,
    pub ends_inning: bool,
    pub batter_count: usize,
    pub batter_name: &'a str,
    pub pitcher_name: &'a str,
    pub fielder_names: Vec<&'a str>,

    pub detail_type: TaxaEventType,
    
    pub advances: Vec<()>
}

enum GamePhase {
    ExpectInningStart,
    ExpectBatterUp,
    ExpectPitch,
    ExpectOutcome(usize)
}

pub struct Game<'g, IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)>> {
    // Should never change
    events: ParsedEventIter<'g, IterT>,
    game_id: &'g str,
    
    // Should not change most of the time, but may change occasionally 
    // due to augments
    away_pitcher_name: &'g str,
    home_pitcher_name: &'g str,
    away_lineup: Vec<PositionedPlayer<&'g str>>,
    home_lineup: Vec<PositionedPlayer<&'g str>>,
    
    // Change all the time
    prev_event_type: ParsedEventDiscriminants,
    phase: GamePhase,
    inning_number: u8,
    inning_half: TopBottom,
    count_balls: u8,
    count_strikes: u8,
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
    
    pub fn next(&mut self, expected: &'static [ParsedEventDiscriminants]) -> Result<(usize, ParsedEvent<&'game str>), SimError> {
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
    ($iter:expr, $([$expected:expr] $p:pat => $e:expr),+) => {{
        let previous = $iter.prev_event_type;
        let expected = &[$($expected,)*];
        match $iter.next(expected)? {
            $($p => Ok($e),)*
            (_, other) => Err(SimError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            })
        }
    }};
    ($iter:expr, $([$expected:expr] $p:pat => $e:expr,)+) => {
        extract_next!($iter, $([$expected] $p => $e),+)
    };
}

struct EventDetailBuilder<'a, 'g, IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)>> {
    game: &'a Game<'g, IterT>,
    contact_event_index: Option<usize>,
    game_event_index: usize,
    advances: Vec<()>,
}

impl<'a, 'g, IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)>> EventDetailBuilder<'a, 'g, IterT> {
    pub fn with_fielder() {
        todo!()
    }

    fn contact_event_index(mut self, contact_event_index: usize) -> Self {
        self.contact_event_index = Some(contact_event_index);
        self
    }
    
    pub fn build_some(self, type_detail: TaxaEventType) -> Option<EventDetail<'g>> {
        Some(self.build(type_detail))
    }

    pub fn build(self, type_detail: TaxaEventType) -> EventDetail<'g> {
        EventDetail {
            game_id: self.game.game_id,
            game_event_index: self.game_event_index,
            contact_game_event_index: self.contact_event_index,
            inning: self.game.inning_number,
            top_of_inning: self.game.inning_half.is_top(),
            count_balls: self.game.count_balls,
            count_strikes: self.game.count_strikes,
            outs_before: self.game.outs,
            outs_after: self.game.outs,
            ends_inning: false,
            batter_count: self.game.batter_count
                .expect("sim::Game state machine should ensure batter_count is set before any EventDetail is constructed"),
            batter_name: self.game.batter_name
                .expect("sim::Game state machine should ensure batter_name is set before any EventDetail is constructed"),
            pitcher_name: self.game.active_pitcher_name(),
            fielder_names: vec![],
            detail_type: type_detail,
            advances: self.advances,
        }
    }

}

impl<'g, IterT> Game<'g, IterT>
where IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)> {
    // TODO Figure out how to accept a simple iterator of ParsedEvent and
    //   do the enumerate myself
    pub fn new(game_id: &'g str, events: IterT) -> Result<Game<'g, IterT>, SimError> {
        let mut events = ParsedEventIter::new(events);

        // TODO Every time there's a { .. } in the match arm of an 
        //   extract_next!, extract the data and issue a warning if it
        //   doesn't match what it should
        extract_next!(
            events,
            [ParsedEventDiscriminants::LiveNow] (_, ParsedEvent::LiveNow { .. }) => ()
        )?;
        
        let (home_pitcher_name, away_pitcher_name) = extract_next!(
            events,
            [ParsedEventDiscriminants::PitchingMatchup] 
            (_, ParsedEvent::PitchingMatchup { home_pitcher, away_pitcher, .. }) => {
                (home_pitcher, away_pitcher)
            }
        )?;
        
        let away_lineup = extract_next!(
            events,
            [ParsedEventDiscriminants::Lineup]
            (_, ParsedEvent::Lineup { side: HomeAway::Away, players }) => players
        )?;
        
        let home_lineup = extract_next!(
            events,
            [ParsedEventDiscriminants::Lineup]
            (_, ParsedEvent::Lineup { side: HomeAway::Home, players }) => players
        )?;

        extract_next!(
            events,
            [ParsedEventDiscriminants::PlayBall]
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
            inning_half: TopBottom::Bottom,
            count_balls: 0,
            count_strikes: 0,
            outs: 0,
            batter_count: None,
            batter_name: None,
        })
    }

    fn active_lineup(&self) -> &[PositionedPlayer<&'g str>] {
        match self.inning_half {
            TopBottom::Top => &self.away_lineup,
            TopBottom::Bottom => &self.home_lineup,
        }
    }
    
    fn active_pitcher_name(&self) -> &'g str {
        match self.inning_half {
            TopBottom::Top => &self.away_pitcher_name,
            TopBottom::Bottom => &self.home_pitcher_name,
        }
    }
    
    fn check_count(&self, (balls, strikes): (u8, u8)) {
        if self.count_balls != balls {
            warn!("Unexpected number of balls in {}: expected {}, but saw {balls}", self.game_id, self.count_balls);
        }
        if self.count_strikes != strikes {
            warn!("Unexpected number of strikes in {}: expected {}, but saw {strikes}", self.game_id, self.count_strikes);
        }
    }
    
    fn detail_builder<'a>(&'a self, game_event_index: usize) -> EventDetailBuilder<'a, 'g, IterT> {
        EventDetailBuilder {
            game: self,
            contact_event_index: None,
            game_event_index,
            advances: Vec::new(),
        }
    }
    
    pub fn finish_pa(&mut self) {
        self.count_strikes = 0;
        self.count_balls = 0;
    }

    // TODO Every time there's a { .. } in the match arm of an 
    //   extract_next!, extract the data and issue a warning if it
    //   doesn't match what it should
    pub fn next(&mut self) -> Result<Option<EventDetail>, SimError> {
        match self.phase {
            GamePhase::ExpectInningStart => {
                let (number, side) = extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::InningStart]
                    (_, ParsedEvent::InningStart { number, side, .. }) => (number, side),
                )?;
                
                if number != self.inning_number + 1 {
                    warn!("Unexpected inning number in {}: expected {}, but saw {number}", self.game_id, self.inning_number + 1)
                }
                self.inning_number = number;
                
                if side != self.inning_half.flip() {
                    warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, self.inning_half.flip())
                }
                self.inning_half = side;
                
                self.phase = GamePhase::ExpectBatterUp;
                Ok(None)
            }
            GamePhase::ExpectBatterUp => {
                let batter_name = extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::NowBatting]
                    (_, ParsedEvent::NowBatting { batter, .. }) => batter,
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
                let predicted_batter_name = lineup[batter_count % lineup.len()].name;
                if batter_name != predicted_batter_name {
                    warn!("Unexpected batter up in {}: expected {predicted_batter_name}, but saw {batter_name}", self.game_id);
                }

                self.phase = GamePhase::ExpectPitch;
                Ok(None)
            }
            GamePhase::ExpectPitch => {
                extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::Ball]
                    (index, ParsedEvent::Ball { steals, count }) => {
                        self.count_balls += 1;
                        self.check_count(count);

                        assert_eq!(steals.len(), 0, "TODO Handle steals");
                        self.detail_builder(index)
                            .build_some(TaxaEventType::Ball)
                    },
                    [ParsedEventDiscriminants::Strike]
                    (index, ParsedEvent::Strike { strike, steals, count }) => {
                        self.count_strikes += 1;
                        self.check_count(count);

                        assert_eq!(steals.len(), 0, "TODO Handle steals");
                        self.detail_builder(index)
                            .build_some(match strike {
                                StrikeType::Looking => { TaxaEventType::StrikeLooking }
                                StrikeType::Swinging => { TaxaEventType::StrikeSwinging }
                            })
                    },
                    [ParsedEventDiscriminants::Foul]
                    (index, ParsedEvent::Foul { foul, steals, count }) => {
                        // Falsehoods...
                        if foul == FoulType::Ball && self.count_strikes < 2 {
                            self.count_strikes += 1;
                        }
                        self.check_count(count);

                        assert_eq!(steals.len(), 0, "TODO Handle steals");
                        self.detail_builder(index)
                            .build_some(match foul {
                                FoulType::Tip => TaxaEventType::FoulTip,
                                FoulType::Ball => TaxaEventType::FoulBall,
                            })
                    },
                    [ParsedEventDiscriminants::Hit]
                    (index, ParsedEvent::Hit { batter: batter_name, hit, destination  }) => {
                        if Some(batter_name) != self.batter_name {
                            if let Some(stored_batter_name) = self.batter_name {
                                warn!("Unexpected batter name in Hit: Expected {stored_batter_name}, but saw {batter_name} ");
                            }
                        }
                        
                        self.phase = GamePhase::ExpectOutcome(index);
                        None
                    },
                )
            }
            GamePhase::ExpectOutcome(contact_event_index) => {
                extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::CaughtOut]
                    (index, ParsedEvent::CaughtOut { batter, hit, catcher, scores, advances, sacrifice, perfect}) => {
                        self.phase = GamePhase::ExpectBatterUp;
                        self.finish_pa();
                        self.detail_builder(index)
                            .contact_event_index(contact_event_index)
                            .build_some(TaxaEventType::Out) // TODO Different out types?
                    },
                )

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