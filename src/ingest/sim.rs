use crate::db::{TaxaEventType, TaxaHitType};
use log::{debug, info, warn};
use mmolb_parsing::ParsedEvent;
use mmolb_parsing::enums::{Distance, FoulType, HomeAway, StrikeType, TopBottom};
use mmolb_parsing::parsed_event::{ParsedEventDiscriminants, PositionedPlayer};
use std::fmt::Debug;
use strum::IntoDiscriminant;
use thiserror::Error;

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

    pub advances: Vec<()>,
}

#[derive(Debug)]
enum GamePhase {
    ExpectInningStart,
    ExpectNowBatting,
    ExpectPitch,
    ExpectFairBallOutcome(usize),
    ExpectInningEnd,
    ExpectMoundVisitOutcome,
    ExpectGameEnd,
    ExpectFinalScore,
    Finished,
}

#[derive(Debug)]
pub struct TeamInGame<'g> {
    team_name: &'g str,
    team_emoji: &'g str,
    pitcher_name: &'g str,
    lineup: Vec<PositionedPlayer<&'g str>>,
    batter_count: Option<usize>,
}

#[derive(Debug)]
pub struct Game<'g> {
    // Should never change
    game_id: &'g str,

    // Aggregates
    away: TeamInGame<'g>,
    home: TeamInGame<'g>,

    // Change all the time
    prev_event_type: ParsedEventDiscriminants,
    phase: GamePhase,
    inning_number: u8,
    inning_half: TopBottom,
    count_balls: u8,
    count_strikes: u8,
    previous_outs: i32,
    outs: i32,
    active_batter_name: Option<&'g str>,
}

#[derive(Debug)]
struct ParsedEventIter<'g, 'a, IterT: Iterator<Item = ParsedEvent<&'g str>>> {
    inner: &'a mut IterT,
    prev_event_type: Option<ParsedEventDiscriminants>,
}

impl<'g, 'a, IterT> ParsedEventIter<'g, 'a, IterT>
where
    IterT: Iterator<Item = ParsedEvent<&'g str>>,
{
    pub fn new(iter: &'a mut IterT) -> Self {
        Self {
            prev_event_type: None,
            inner: iter,
        }
    }

    pub fn next(
        &mut self,
        expected: &'static [ParsedEventDiscriminants],
    ) -> Result<ParsedEvent<&'g str>, SimError> {
        match self.inner.next() {
            Some(val) => {
                self.prev_event_type = Some(val.discriminant());
                Ok(val)
            }
            None => match self.prev_event_type {
                None => Err(SimError::NoEvents),
                Some(previous) => Err(SimError::NotEnoughEvents { expected, previous }),
            },
        }
    }
}

// This macro accepts an iterator over game events, and is meant for
// use in Game::new. See game_event! for the equivalent that's meant
// for use in Game::next.
macro_rules! extract_next_game_event {
    // This arm matches when there isn't a trailing comma, adds the
    // trailing comma, and forwards to the other arm
    ($iter:expr, $([$expected:expr] $p:pat => $e:expr,)+) => {
        extract_next_game_event!($iter, $([$expected] $p => $e),+)
    };
    // This arm matches when there is a trailing comma
    ($iter:expr, $([$expected:expr] $p:pat => $e:expr),+) => {{
        let previous = $iter.prev_event_type;
        let expected = &[$($expected,)*];
        match $iter.next(expected)? {
            $($p => Ok($e),)*
            other => Err(SimError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            })
        }
    }};
}

// This macro accepts a Game and a game event, and is meant for
// use in Game::next. See extract_next_game_event! for the equivalent
// that's meant for use in Game::new.

macro_rules! game_event {
    // This is the main arm, and matches when there is a trailing comma.
    // It needs to be first, otherwise the other two arms will be
    // infinitely mutually recursive.
    (($previous_event:expr, $event:expr), $([$expected:expr] $p:pat => $e:expr,)*) => {{
        // This is wrapped in Some because SimError::UnexpectedEventType
        // takes an Option to handle the case when the error is at the
        // first event (and therefore there is no previous event).
        // However, this macro is only used on a fully-constructed Game,
        // which must have a previous event. So prev_event_type is not
        // an Option, but previous must be.
        let previous: Option<ParsedEventDiscriminants> = Some($previous_event);
        let expected: &[ParsedEventDiscriminants] = &[$($expected,)*];

        match $event {
            $($p => {
                Ok($e)
            })*
            other => Err(SimError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            })
        }
    }};
    // This arm matches when there isn't a trailing comma, adds the
    // trailing comma, and forwards to the main arm
    (($previous_event:expr, $event:expr), $([$expected:expr] $p:pat => $e:expr),*) => {
        game_event!(($previous_event, $event), $([$expected] $p => $e,)*)
    };
    // This arm matches when there are no patterns provided and no
    // trailing comma (no patterns with trailing comma is captured by
    // the previous arm)
    (($previous_event:expr, $event:expr)) => {
        game_event!(($previous_event, $event),)
    };
}

struct EventDetailBuilder<'a, 'g> {
    game: &'a Game<'g>,
    contact_event_index: Option<usize>,
    game_event_index: usize,
    advances: Vec<()>,
    hit_type: Option<TaxaHitType>,
}

impl<'a, 'g> EventDetailBuilder<'a, 'g> {
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
            batter_count: self.game.batting_team().batter_count
                .expect("sim::Game state machine should ensure batter_count is set before any EventDetail is constructed"),
            batter_name: self.game.active_batter_name
                .expect("sim::Game state machine should ensure batter_name is set before any EventDetail is constructed"),
            pitcher_name: self.game.defending_team().pitcher_name,
            fielder_names: vec![],
            detail_type: type_detail,
            hit_type: self.hit_type,
            advances: self.advances,
        }
    }
}

impl<'g> Game<'g> {
    pub fn new<IterT>(game_id: &'g str, events: &mut IterT) -> Result<Game<'g>, SimError>
    where
        IterT: Iterator<Item = ParsedEvent<&'g str>>,
    {
        let mut events = ParsedEventIter::new(events);

        let (away_team_name, away_team_emoji, home_team_name, home_team_emoji) = extract_next_game_event!(
            events,
            [ParsedEventDiscriminants::LiveNow]
            ParsedEvent::LiveNow {
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            } => (
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            )
        )?;

        let (
            home_pitcher_name,
            away_pitcher_name,
            away_team_name_2,
            away_team_emoji_2,
            home_team_name_2,
            home_team_emoji_2,
        ) = extract_next_game_event!(
            events,
            [ParsedEventDiscriminants::PitchingMatchup]
            ParsedEvent::PitchingMatchup {
                home_pitcher,
                away_pitcher,
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            } => (
                home_pitcher,
                away_pitcher,
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            )
        )?;
        if away_team_name_2 != away_team_name {
            warn!(
                "Away team name from PitchingMatchup ({away_team_name_2}) did \
                not match the one from LiveNow ({away_team_name})"
            );
        }
        if away_team_emoji_2 != away_team_emoji {
            warn!(
                "Away team emoji from PitchingMatchup ({away_team_emoji_2}) did \
                not match the one from LiveNow ({away_team_emoji})"
            );
        }
        if home_team_name_2 != home_team_name {
            warn!(
                "Home team name from PitchingMatchup ({home_team_name_2}) did \
                not match the one from LiveNow ({home_team_name})"
            );
        }
        if home_team_emoji_2 != home_team_emoji {
            warn!(
                "Home team emoji from PitchingMatchup ({home_team_emoji_2}) did \
                not match the one from LiveNow ({home_team_emoji})"
            );
        }

        let away_lineup = extract_next_game_event!(
            events,
            [ParsedEventDiscriminants::Lineup]
            ParsedEvent::Lineup { side: HomeAway::Away, players } => players
        )?;

        let home_lineup = extract_next_game_event!(
            events,
            [ParsedEventDiscriminants::Lineup]
            ParsedEvent::Lineup { side: HomeAway::Home, players } => players
        )?;

        extract_next_game_event!(
            events,
            [ParsedEventDiscriminants::PlayBall]
            ParsedEvent::PlayBall => ()
        )?;

        Ok(Self {
            game_id,
            away: TeamInGame {
                team_name: away_team_name,
                team_emoji: away_team_emoji,
                pitcher_name: away_pitcher_name,
                lineup: away_lineup,
                batter_count: None,
            },
            home: TeamInGame {
                team_name: home_team_name,
                team_emoji: home_team_emoji,
                pitcher_name: home_pitcher_name,
                lineup: home_lineup,
                batter_count: None,
            },
            prev_event_type: ParsedEventDiscriminants::PlayBall,
            phase: GamePhase::ExpectInningStart,
            inning_number: 0,
            inning_half: TopBottom::Bottom,
            count_balls: 0,
            count_strikes: 0,
            previous_outs: 0,
            outs: 0,
            // TODO Get this from lineup and batter count
            active_batter_name: None,
        })
    }

    fn batting_team(&self) -> &TeamInGame<'g> {
        match self.inning_half {
            TopBottom::Top => &self.away,
            TopBottom::Bottom => &self.home,
        }
    }

    fn batting_team_mut(&mut self) -> &mut TeamInGame<'g> {
        match self.inning_half {
            TopBottom::Top => &mut self.away,
            TopBottom::Bottom => &mut self.home,
        }
    }

    fn defending_team(&self) -> &TeamInGame<'g> {
        match self.inning_half {
            TopBottom::Top => &self.home,
            TopBottom::Bottom => &self.away,
        }
    }

    fn defending_team_mut(&mut self) -> &mut TeamInGame<'g> {
        match self.inning_half {
            TopBottom::Top => &mut self.home,
            TopBottom::Bottom => &mut self.away,
        }
    }

    fn check_count(&self, (balls, strikes): (u8, u8)) {
        if self.count_balls != balls {
            warn!(
                "Unexpected number of balls in {}: expected {}, but saw {balls}",
                self.game_id, self.count_balls
            );
        }
        if self.count_strikes != strikes {
            warn!(
                "Unexpected number of strikes in {}: expected {}, but saw {strikes}",
                self.game_id, self.count_strikes
            );
        }
    }

    fn check_batter(&self, batter_name: &str) {
        if let Some(stored_batter_name) = self.active_batter_name {
            if stored_batter_name != batter_name {
                warn!(
                    "Unexpected batter name in Hit: Expected {}, but saw {}",
                    stored_batter_name
                    batter_name,
                );
            }
        } else {
            warn!("Unexpected batter name in Hit: Expected no batter, but saw {batter_name} ");
        }
    }

    fn detail_builder<'a>(&'a self, game_event_index: usize) -> EventDetailBuilder<'a, 'g> {
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
        self.phase = GamePhase::ExpectNowBatting;
    }

    pub fn add_out(&mut self) {
        debug!("Number of outs at start of event: {}", self.outs);
        self.outs += 1;

        if self.outs >= 3 {
            self.phase = GamePhase::ExpectInningEnd;
        } else {
            self.phase = GamePhase::ExpectNowBatting;
        }
    }

    // TODO Every time there's a { .. } in the match arm of an
    //   extract_next!, extract the data. If it's redundant with
    //   something else, check it against that other thing and issue a
    //   warning if it doesn't match. Otherwise, record the data.
    pub fn next(
        &mut self,
        index: usize,
        event: ParsedEvent<&'g str>,
    ) -> Result<Option<EventDetail>, SimError> {
        self.previous_outs = self.outs;
        let previous_event = self.prev_event_type;
        let this_event_discriminant = event.discriminant();
        let result = match self.phase {
            GamePhase::ExpectInningStart => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::InningStart]
                ParsedEvent::InningStart {
                    number,
                    side,
                    batting_team_emoji,
                    batting_team_name,
                    pitcher_status,
                } => {
                    if side != self.inning_half.flip() {
                        warn!(
                            "Unexpected inning side in {}: expected {:?}, but saw {side:?}",
                            self.game_id,
                            self.inning_half.flip(),
                        );
                    }
                    self.inning_half = side;

                    // If we just started a top, the number should increment
                    let expected_number = match self.inning_half {
                        TopBottom::Top => self.inning_number + 1,
                        TopBottom::Bottom => self.inning_number,
                    };

                    if number != expected_number {
                        warn!(
                            "Unexpected inning number in {}: expected {}, but saw {}",
                            self.game_id,
                            expected_number,
                            number,
                        );
                    }
                    self.inning_number = number;

                    if batting_team_name != self.batting_team().team_name {
                        warn!(
                            "Batting team name from InningStart ({batting_team_name}) did \
                            not match the one from LiveNow ({})",
                            self.batting_team().team_name,
                        );
                    }
                    if batting_team_emoji != self.batting_team().team_emoji {
                        warn!(
                            "Batting team emoji from InningStart ({batting_team_emoji}) did \
                            not match the one from LiveNow ({})",
                            self.batting_team().team_emoji,
                        );
                    }

                    info!(
                        "Started {} of {} with pitcher {pitcher_status:?}",
                        self.inning_half,
                        self.inning_number,
                    );

                    self.outs = 0;
                    self.phase = GamePhase::ExpectNowBatting;
                    None
                },
                [ParsedEventDiscriminants::MoundVisit]
                ParsedEvent::MoundVisit { emoji, team } => {
                    if team != self.defending_team().team_name {
                        warn!(
                            "Batting team name from MoundVisit ({team}) did \
                            not match the one from LiveNow ({})",
                            self.defending_team().team_name,
                        );
                    }
                    if emoji != self.defending_team().team_emoji {
                        warn!(
                            "Batting team emoji from MoundVisit ({emoji}) did \
                            not match the one from LiveNow ({})",
                            self.defending_team().team_emoji,
                        );
                    }

                    self.phase = GamePhase::ExpectMoundVisitOutcome;
                    None
                },
            ),
            GamePhase::ExpectNowBatting => game_event!(
               (previous_event, event),
               [ParsedEventDiscriminants::NowBatting]
               // TODO handle every single member of this variant
               ParsedEvent::NowBatting { batter: batter_name, .. } => {
                   self.active_batter_name = Some(batter_name);
                   let batter_count = if let Some(batter_count) = &mut self.batting_team_mut().batter_count {
                       *batter_count += 1;
                       *batter_count
                   } else {
                       self.batting_team_mut().batter_count = Some(0);
                       0
                   };

                   let lineup = &self.batting_team().lineup;
                   let predicted_batter_name = lineup[batter_count % lineup.len()].name;
                   if batter_name != predicted_batter_name {
                       warn!(
                            "Unexpected batter up in {}: expected {}, but saw {}",
                            self.game_id,
                            predicted_batter_name,
                            batter_name,
                       );
                   }

                   self.phase = GamePhase::ExpectPitch;
                   None
               },
               [ParsedEventDiscriminants::MoundVisit]
               // TODO handle every single member of this variant
               ParsedEvent::MoundVisit { .. } => {
                   self.phase = GamePhase::ExpectMoundVisitOutcome;
                   None
               },
            ),
            GamePhase::ExpectPitch => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::Ball]
                // TODO handle every single member of this variant
                ParsedEvent::Ball { count, .. } => {
                    self.count_balls += 1;
                    self.check_count(count);

                    self.detail_builder(index)
                        .build_some(TaxaEventType::Ball)
                },
                [ParsedEventDiscriminants::Strike]
                // TODO handle every single member of this variant
                ParsedEvent::Strike { strike, count, .. } => {
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
                ParsedEvent::StrikeOut { strike, .. } => {
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
                ParsedEvent::Foul { foul, count, .. } => {
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
                ParsedEvent::Hit { batter, .. } => {
                    self.check_batter(batter);

                    self.phase = GamePhase::ExpectFairBallOutcome(index);
                    None
                },
                [ParsedEventDiscriminants::Walk]
                // TODO handle every single member of this variant
                ParsedEvent::Walk { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();

                    self.detail_builder(index)
                        .build_some(TaxaEventType::Walk)
                },
                [ParsedEventDiscriminants::HitByPitch]
                // TODO handle every single member of this variant
                ParsedEvent::HitByPitch { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();

                    self.detail_builder(index)
                        .build_some(TaxaEventType::HitByPitch)
                },
            ),
            GamePhase::ExpectFairBallOutcome(contact_event_index) => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::CaughtOut]
                // TODO handle every single member of this variant
                ParsedEvent::CaughtOut { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();
                    self.add_out();
                    self.detail_builder(index)
                        .contact_event_index(contact_event_index)
                        .build_some(TaxaEventType::Out) // TODO Different out types?
                },
                [ParsedEventDiscriminants::GroundedOut]
                // TODO handle every single member of this variant
                ParsedEvent::GroundedOut { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();
                    self.add_out();
                    self.detail_builder(index)
                        .contact_event_index(contact_event_index)
                        .build_some(TaxaEventType::Out) // TODO Different out types?
                },
                [ParsedEventDiscriminants::BatterToBase]
                // TODO handle every single member of this variant
                ParsedEvent::BatterToBase { batter, distance, ..  } => {
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
                ParsedEvent::FieldingError { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();

                    self.detail_builder(index)
                        .contact_event_index(contact_event_index)
                        .build_some(TaxaEventType::FieldingError)
                },
                [ParsedEventDiscriminants::Homer]
                // TODO handle every single member of this variant
                ParsedEvent::Homer { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();

                    self.detail_builder(index)
                        .contact_event_index(contact_event_index)
                        .build_some(TaxaEventType::HomeRun)
                },
                [ParsedEventDiscriminants::DoublePlay]
                // TODO handle every single member of this variant
                ParsedEvent::DoublePlay { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();
                    self.add_out();
                    self.add_out(); // It's a double play -- we add two outs

                    self.detail_builder(index)
                        .contact_event_index(contact_event_index)
                        .build_some(TaxaEventType::DoublePlay)
                },
                [ParsedEventDiscriminants::ForceOut]
                // TODO handle every single member of this variant
                ParsedEvent::ForceOut { batter, .. } => {
                    self.check_batter(batter);
                    self.finish_pa();
                    self.add_out();

                    self.detail_builder(index)
                        .contact_event_index(contact_event_index)
                        .build_some(TaxaEventType::Out) // TODO Different out types?
                },
            ),
            GamePhase::ExpectInningEnd => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::InningEnd]
                ParsedEvent::InningEnd { number, side } => {
                    if number != self.inning_number {
                        warn!("Unexpected inning number in {}: expected {}, but saw {number}", self.game_id, self.inning_number);
                    }

                    if side != self.inning_half {
                        warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, self.inning_half);
                    }

                    // TODO Implement the full extra innings rule
                    if number == 9 {
                        self.phase = GamePhase::ExpectGameEnd;
                    } else {
                        self.phase = GamePhase::ExpectInningStart;
                    }
                    None
                },
            ),
            GamePhase::ExpectMoundVisitOutcome => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::PitcherRemains]
                // TODO handle every single member of this variant
                ParsedEvent::PitcherRemains { .. } => {
                    // I think this is not always ExpectNowBatting. I may have
                    // to store the state-to-return-to as a data member of
                    // GamePhase::ExpectMoundVisitOutcome
                    self.phase = GamePhase::ExpectNowBatting;
                    None
                },
                [ParsedEventDiscriminants::PitcherSwap]
                // TODO handle every single member of this variant
                ParsedEvent::PitcherSwap { .. } => {
                    // I think this is not always ExpectNowBatting. I may have
                    // to store the state-to-return-to as a data member of
                    // GamePhase::ExpectMoundVisitOutcome
                    self.phase = GamePhase::ExpectNowBatting;
                    None
                },
            ),
            GamePhase::ExpectGameEnd => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::PitcherSwap]
                ParsedEvent::GameOver => {
                    self.phase = GamePhase::ExpectFinalScore;
                    None
                },
            ),
            GamePhase::ExpectFinalScore => game_event!(
                (previous_event, event),
                [ParsedEventDiscriminants::Recordkeeping]
                // TODO handle every single member of this variant
                ParsedEvent::Recordkeeping { .. } => {
                    self.phase = GamePhase::Finished;
                    None
                },
            ),
            GamePhase::Finished => game_event!((previous_event, event)),
        }?;

        self.prev_event_type = this_event_discriminant;

        Ok(result)
    }
}
