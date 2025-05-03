use std::fmt::Debug;
use log::{debug, info, warn};
use mmolb_parsing::enums::{Distance, FoulType, HomeAway, StrikeType, TopBottom};
use mmolb_parsing::ParsedEvent;
use mmolb_parsing::parsed_event::{ParsedEventDiscriminants, PositionedPlayer};
use strum::IntoDiscriminant;
use thiserror::Error;
use crate::db::{TaxaEventType, TaxaHitType};

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
    pub hit_type: Option<TaxaHitType>,

    pub advances: Vec<()>
}

#[derive(Debug)]
enum GamePhase {
    ExpectInningStart,
    ExpectBatterUp,
    ExpectPitch,
    ExpectOutcome(usize),
    ExpectInningEnd,
}

#[derive(Debug)]
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
    previous_outs: i32,
    outs: i32,
    away_batter_count: Option<usize>,
    home_batter_count: Option<usize>,
    batter_name: Option<&'g str>,
}

#[derive(Debug)]
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
    hit_type: Option<TaxaHitType>,
}

impl<'a, 'g, IterT: Iterator<Item=(usize, ParsedEvent<&'g str>)>> EventDetailBuilder<'a, 'g, IterT> {
    fn contact_event_index(mut self, contact_event_index: usize) -> Self {
        self.contact_event_index = Some(contact_event_index);
        self
    }

    fn hit_type(mut self, hit_type: TaxaHitType) -> Self {
        self.hit_type = Some(hit_type);
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
            batter_count: self.game.active_batter_count()
                .expect("sim::Game state machine should ensure batter_count is set before any EventDetail is constructed"),
            batter_name: self.game.batter_name
                .expect("sim::Game state machine should ensure batter_name is set before any EventDetail is constructed"),
            pitcher_name: self.game.active_pitcher_name(),
            fielder_names: vec![],
            detail_type: type_detail,
            hit_type: self.hit_type,
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
            previous_outs: 0,
            outs: 0,
            away_batter_count: None,
            home_batter_count: None,
            batter_name: None,
        })
    }

    fn active_batter_count(&self) -> &Option<usize> {
        match self.inning_half {
            TopBottom::Top => &self.away_batter_count,
            TopBottom::Bottom => &self.home_batter_count,
        }
    }

    fn active_batter_count_mut(&mut self) -> &mut Option<usize> {
        match self.inning_half {
            TopBottom::Top => &mut self.away_batter_count,
            TopBottom::Bottom => &mut self.home_batter_count,
        }
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
    
    fn check_batter(&self, batter_name: &str) {
        if let Some(stored_batter_name) = self.batter_name {
            if stored_batter_name != batter_name {
                warn!("Unexpected batter name in Hit: Expected {stored_batter_name}, but saw {batter_name} ");
            }
        } else {
            warn!("Unexpected batter name in Hit: Expected no batter, but saw {batter_name} ");
        }
    }
    
    fn detail_builder<'a>(&'a self, game_event_index: usize) -> EventDetailBuilder<'a, 'g, IterT> {
        EventDetailBuilder {
            game: self,
            contact_event_index: None,
            game_event_index,
            advances: Vec::new(),
            hit_type: None,
        }
    }

    pub fn finish_pa(&mut self) {
        self.count_strikes = 0;
        self.count_balls = 0;
        self.phase = GamePhase::ExpectBatterUp;
    }

    pub fn add_out(&mut self) {
        debug!("Number of outs at start of event: {}", self.outs);
        self.outs += 1;

        if self.outs >= 3 {
            self.phase = GamePhase::ExpectInningEnd;
        } else {
            self.phase = GamePhase::ExpectBatterUp;
        }
    }

    // TODO Every time there's a { .. } in the match arm of an 
    //   extract_next!, extract the data and issue a warning if it
    //   doesn't match what it should
    pub fn next(&mut self) -> Result<Option<EventDetail>, SimError>
        where IterT: Debug {
        self.previous_outs = self.outs;
        match self.phase {
            GamePhase::ExpectInningStart => {
                let (number, side) = extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::InningStart]
                    (_, ParsedEvent::InningStart { number, side, .. }) => (number, side),
                )?;

                if side != self.inning_half.flip() {
                    warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, self.inning_half.flip())
                }
                self.inning_half = side;

                // If we just started a top, the number should increment
                let expected_number = match self.inning_half {
                    TopBottom::Top => self.inning_number + 1,
                    TopBottom::Bottom => self.inning_number,
                };
                
                if number != expected_number {
                    warn!("Unexpected inning number in {}: expected {expected_number}, but saw {number}", self.game_id);
                }
                self.inning_number = number;

                info!("Started {} of {}", self.inning_half, self.inning_number);

                self.outs = 0;
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
                let batter_count = if let Some(batter_count) = self.active_batter_count_mut() {
                    *batter_count += 1;
                    *batter_count
                } else {
                    *self.active_batter_count_mut() = Some(0);
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
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::Ball { count, .. }) => {
                        self.count_balls += 1;
                        self.check_count(count);

                        self.detail_builder(index)
                            .build_some(TaxaEventType::Ball)
                    },
                    [ParsedEventDiscriminants::Strike]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::Strike { strike, count, .. }) => {
                        self.count_strikes += 1;
                        self.check_count(count);

                        self.detail_builder(index)
                            .build_some(match strike {
                                StrikeType::Looking => { TaxaEventType::StrikeLooking }
                                StrikeType::Swinging => { TaxaEventType::StrikeSwinging }
                            })
                    },
                    [ParsedEventDiscriminants::StrikeOut]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::StrikeOut { strike, ..  }) => {
                        if self.count_strikes < 2 {
                            warn!("Unexpected strikeout in {}: expected 2 strikes in the count, but there were {}", self.game_id, self.count_strikes);
                        }

                        self.finish_pa();
                        self.add_out();

                        self.detail_builder(index)
                            .build_some(match strike {
                                StrikeType::Looking => { TaxaEventType::StrikeLooking }
                                StrikeType::Swinging => { TaxaEventType::StrikeSwinging }
                            })
                    },
                    [ParsedEventDiscriminants::Foul]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::Foul { foul, count, .. }) => {
                        // Falsehoods...
                        if !(foul == FoulType::Ball && self.count_strikes >= 2) {
                            self.count_strikes += 1;
                        }
                        self.check_count(count);

                        self.detail_builder(index)
                            .build_some(match foul {
                                FoulType::Tip => TaxaEventType::FoulTip,
                                FoulType::Ball => TaxaEventType::FoulBall,
                            })
                    },
                    // Note this is NOT a baseball Hit. This is a batted ball.
                    [ParsedEventDiscriminants::Hit]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::Hit { batter, .. }) => {
                        self.check_batter(batter);

                        self.phase = GamePhase::ExpectOutcome(index);
                        None
                    },
                    [ParsedEventDiscriminants::Walk]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::Walk { batter, .. }) => {
                        self.check_batter(batter);
                        self.finish_pa();

                        self.detail_builder(index)
                            .build_some(TaxaEventType::Walk)
                    },
                    [ParsedEventDiscriminants::HitByPitch]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::HitByPitch { batter, .. }) => {
                        self.check_batter(batter);
                        self.finish_pa();

                        self.detail_builder(index)
                            .build_some(TaxaEventType::HitByPitch)
                    },
                )
            }
            GamePhase::ExpectOutcome(contact_event_index) => {
                extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::CaughtOut]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::CaughtOut { batter, .. }) => {
                        self.check_batter(batter);
                        self.finish_pa();
                        self.add_out();
                        self.detail_builder(index)
                            .contact_event_index(contact_event_index)
                            .build_some(TaxaEventType::Out) // TODO Different out types?
                    },
                    [ParsedEventDiscriminants::GroundedOut]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::GroundedOut { batter, .. }) => {
                        self.check_batter(batter);
                        self.finish_pa();
                        self.add_out();
                        self.detail_builder(index)
                            .contact_event_index(contact_event_index)
                            .build_some(TaxaEventType::Out) // TODO Different out types?
                    },
                    [ParsedEventDiscriminants::BatterToBase]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::BatterToBase { batter, distance, ..  }) => {
                        self.check_batter(batter);
                        self.finish_pa();
                        
                        match match distance {
                            Distance::Single => { Some(TaxaHitType::Single) }
                            Distance::Double => { Some(TaxaHitType::Double) }
                            Distance::Triple => { Some(TaxaHitType::Triple) }
                            Distance::HomeRun => { None }
                        } {
                            // Hit
                            Some(hit_type) => {
                                self.detail_builder(index)
                                    .contact_event_index(contact_event_index)
                                    .hit_type(hit_type)
                                    .build_some(TaxaEventType::Hit)
                            }
                            // Home run
                            None => {
                                assert!(false, "Encountered a BatterToBase with Distance::HomeRun. Need to figure out what makes this different from a Homer");
                                self.detail_builder(index)
                                    .contact_event_index(contact_event_index)
                                    .build_some(TaxaEventType::HomeRun)
                            }
                        }
                    },
                    [ParsedEventDiscriminants::FieldingError]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::FieldingError { batter, .. }) => {
                        self.check_batter(batter);
                        self.finish_pa();
                        
                        self.detail_builder(index)
                            .contact_event_index(contact_event_index)
                            .build_some(TaxaEventType::FieldingError)
                    },
                    [ParsedEventDiscriminants::Homer]
                    // TODO handle every single member of this variant
                    (index, ParsedEvent::Homer { batter, .. }) => {
                        self.check_batter(batter);
                        self.finish_pa();
                        
                        self.detail_builder(index)
                            .contact_event_index(contact_event_index)
                            .build_some(TaxaEventType::HomeRun)
                    },
                )
            }
            GamePhase::ExpectInningEnd => {
                let (number, side) = extract_next!(
                    self.events,
                    [ParsedEventDiscriminants::InningEnd]
                    (_, ParsedEvent::InningEnd { number, side }) => (number, side),
                )?;

                if number != self.inning_number {
                    warn!("Unexpected inning number in {}: expected {}, but saw {number}", self.game_id, self.inning_number);
                }

                if side != self.inning_half {
                    warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, self.inning_half);
                }

                self.phase = GamePhase::ExpectInningStart;
                Ok(None)
            }
        }
    }
}