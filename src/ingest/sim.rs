use crate::db::{TaxaEventType, TaxaFairBallType, TaxaPosition, TaxaHitType};
use itertools::Itertools;
use log::{debug, info, warn};
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::{
    Distance, FieldingErrorType, FoulType, HitDestination, HitType, HomeAway, StrikeType, TopBottom,
};
use mmolb_parsing::parsed_event::{ParsedEventMessageDiscriminants, PositionedPlayer, RunnerAdvance};
use std::fmt::Debug;
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimError {
    #[error("This game had no events")]
    NoEvents,

    #[error("Not enough events. Expected {expected:?} event after {previous:?}")]
    NotEnoughEvents {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: ParsedEventMessageDiscriminants,
    },

    #[error("Expected {expected:?} event after {previous:?}, but received {received:?}")]
    UnexpectedEventType {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: Option<ParsedEventMessageDiscriminants>,
        received: ParsedEventMessageDiscriminants,
    },
}

#[derive(Debug)]
pub struct EventDetail<StrT> {
    pub game_id: StrT,
    pub game_event_index: usize,
    pub fair_ball_event_index: Option<usize>,
    pub inning: u8,
    pub top_of_inning: bool,
    pub count_balls: u8,
    pub count_strikes: u8,
    pub outs_before: i32,
    pub outs_after: i32,
    pub batter_count: usize,
    pub batter_name: StrT,
    pub pitcher_name: StrT,
    pub fielders: Vec<PositionedPlayer<StrT>>,

    pub detail_type: TaxaEventType,
    pub hit_type: Option<TaxaHitType>,
    pub fair_ball_type: Option<TaxaFairBallType>,
    pub fair_ball_direction: Option<TaxaPosition>,

    pub advances: Vec<RunnerAdvance<StrT>>,
}

#[derive(Debug, Copy, Clone)]
struct FairBall {
    index: usize,
    hit_type: HitType,
    hit_destination: HitDestination
}

#[derive(Debug, Copy, Clone)]
enum GamePhase {
    ExpectInningStart,
    ExpectNowBatting,
    ExpectPitch,
    ExpectFairBallOutcome(FairBall),
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
    prev_event_type: ParsedEventMessageDiscriminants,
    phase: GamePhase,
    inning_number: u8,
    inning_half: TopBottom,
    count_balls: u8,
    count_strikes: u8,
    previous_outs: i32,
    outs: i32,
}

#[derive(Debug)]
struct ParsedEventMessageIter<'g, 'a, IterT: Iterator<Item = ParsedEventMessage<&'g str>>> {
    inner: &'a mut IterT,
    prev_event_type: Option<ParsedEventMessageDiscriminants>,
}

impl<'g, 'a, IterT> ParsedEventMessageIter<'g, 'a, IterT>
where
    IterT: Iterator<Item = ParsedEventMessage<&'g str>>,
{
    pub fn new(iter: &'a mut IterT) -> Self {
        Self {
            prev_event_type: None,
            inner: iter,
        }
    }

    pub fn next(
        &mut self,
        expected: &'static [ParsedEventMessageDiscriminants],
    ) -> Result<ParsedEventMessage<&'g str>, SimError> {
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
        let previous: Option<ParsedEventMessageDiscriminants> = Some($previous_event);
        let expected: &[ParsedEventMessageDiscriminants] = &[$($expected,)*];

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
    game_event_index: usize,
    fair_ball_event_index: Option<usize>,
    fair_ball_type: Option<TaxaFairBallType>,
    fair_ball_direction: Option<TaxaPosition>,
    hit_type: Option<TaxaHitType>,
    fielders: Vec<PositionedPlayer<&'g str>>,
    advances: Vec<RunnerAdvance<&'g str>>,
}

impl<'a, 'g> EventDetailBuilder<'a, 'g> {
    fn fair_ball(mut self, fair_ball: FairBall) -> Self {
        self.fair_ball_event_index = Some(fair_ball.index);
        self.fair_ball_type = Some(fair_ball.hit_type.into());
        self.fair_ball_direction = Some(fair_ball.hit_destination.into());
        self
    }

    fn hit_type(mut self, hit_type: TaxaHitType) -> Self {
        self.hit_type = Some(hit_type);
        self
    }

    fn fair_ball_type(mut self, fair_ball_type: TaxaFairBallType) -> Self {
        self.fair_ball_type = Some(fair_ball_type);
        self
    }

    fn fielder(mut self, fielder: PositionedPlayer<&'g str>) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }
        
        self.fielders = vec![fielder];
        self
    }

    fn fielders(
        mut self,
        fielders: Vec<PositionedPlayer<&'g str>>,
    ) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = fielders;
        self
    }

    fn advance(mut self, advance: RunnerAdvance<&'g str>) -> Self {
        if !self.advances.is_empty() {
            warn!("EventDetailBuilder overwrote existing advances");
        }
        
        self.advances = vec![advance];
        self
    }

    fn advances(
        mut self,
        advances: Vec<RunnerAdvance<&'g str>>,
    ) -> Self {
        if !self.advances.is_empty() {
            warn!("EventDetailBuilder overwrote existing advances");
        }

        self.advances = advances;
        self
    }

    pub fn build_some(self, type_detail: TaxaEventType) -> Option<EventDetail<&'g str>> {
        Some(self.build(type_detail))
    }

    pub fn build(self, type_detail: TaxaEventType) -> EventDetail<&'g str> {
        EventDetail {
            game_id: self.game.game_id,
            game_event_index: self.game_event_index,
            fair_ball_event_index: self.fair_ball_event_index,
            inning: self.game.inning_number,
            top_of_inning: self.game.inning_half.is_top(),
            count_balls: self.game.count_balls,
            count_strikes: self.game.count_strikes,
            outs_before: self.game.previous_outs,
            outs_after: self.game.outs,
            batter_count: self.game.batting_team().batter_count
                .expect("sim::Game state machine should ensure batter_count is set before any EventDetail is constructed"),
            batter_name: self.game.active_batter()
                .expect("sim::Game state machine should ensure batter_name is set before any EventDetail is constructed")
                .name,
            pitcher_name: self.game.defending_team().pitcher_name,
            fielders: self.fielders,
            detail_type: type_detail,
            hit_type: self.hit_type,
            fair_ball_type: self.fair_ball_type,
            fair_ball_direction: self.fair_ball_direction,
            advances: self.advances,
        }
    }
}

impl<'g> Game<'g> {
    pub fn new<IterT>(game_id: &'g str, events: &mut IterT) -> Result<Game<'g>, SimError>
    where
        IterT: Iterator<Item = ParsedEventMessage<&'g str>>,
    {
        let mut events = ParsedEventMessageIter::new(events);

        let (away_team_name, away_team_emoji, home_team_name, home_team_emoji) = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::LiveNow]
            ParsedEventMessage::LiveNow {
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
            [ParsedEventMessageDiscriminants::PitchingMatchup]
            ParsedEventMessage::PitchingMatchup {
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
            [ParsedEventMessageDiscriminants::Lineup]
            ParsedEventMessage::Lineup { side: HomeAway::Away, players } => players
        )?;

        let home_lineup = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::Lineup]
            ParsedEventMessage::Lineup { side: HomeAway::Home, players } => players
        )?;

        extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::PlayBall]
            ParsedEventMessage::PlayBall => ()
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
            prev_event_type: ParsedEventMessageDiscriminants::PlayBall,
            phase: GamePhase::ExpectInningStart,
            inning_number: 0,
            inning_half: TopBottom::Bottom,
            count_balls: 0,
            count_strikes: 0,
            previous_outs: 0,
            outs: 0,
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

    fn active_batter(&self) -> Option<&PositionedPlayer<&'g str>> {
        if let Some(batter_count) = self.batting_team().batter_count {
            let lineup = &self.batting_team().lineup;
            Some(&lineup[batter_count % lineup.len()])
        } else {
            None
        }
    }

    fn check_batter(&self, batter_name: &str, event_type: ParsedEventMessageDiscriminants) {
        if let Some(active_batter) = self.active_batter() {
            if active_batter.name != batter_name {
                warn!(
                    "Unexpected batter name in {:#?}: Expected {}, but saw {}",
                    event_type, active_batter.name, batter_name,
                );
            }
        } else {
            warn!(
                "Unexpected batter name in {:#?}: Expected no batter, but saw {batter_name} ",
                event_type,
            );
        }
    }

    fn detail_builder<'a>(&'a self, game_event_index: usize) -> EventDetailBuilder<'a, 'g> {
        EventDetailBuilder {
            game: self,
            fair_ball_event_index: None,
            game_event_index,
            fielders: Vec::new(),
            advances: Vec::new(),
            hit_type: None,
            fair_ball_type: None,
            fair_ball_direction: None,
        }
    }

    // The semantics of this method are that None + 1 == Some(0)
    pub fn increment_batter_count(&mut self) {
        if let Some(batter_count) = &mut self.batting_team_mut().batter_count {
            *batter_count += 1;
        } else {
            self.batting_team_mut().batter_count = Some(0);
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
        event: &ParsedEventMessage<&'g str>,
    ) -> Result<Option<EventDetail<&'g str>>, SimError> {
        self.previous_outs = self.outs;
        let previous_event = self.prev_event_type;
        let this_event_discriminant = event.discriminant();
        let result = match self.phase {
            GamePhase::ExpectInningStart => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningStart]
                ParsedEventMessage::InningStart {
                    number,
                    side,
                    batting_team_emoji,
                    batting_team_name,
                    pitcher_status,
                } => {
                    if *side != self.inning_half.flip() {
                        warn!(
                            "Unexpected inning side in {}: expected {:?}, but saw {side:?}",
                            self.game_id,
                            self.inning_half.flip(),
                        );
                    }
                    self.inning_half = *side;

                    // If we just started a top, the number should increment
                    let expected_number = match self.inning_half {
                        TopBottom::Top => self.inning_number + 1,
                        TopBottom::Bottom => self.inning_number,
                    };

                    if *number != expected_number {
                        warn!(
                            "Unexpected inning number in {}: expected {}, but saw {}",
                            self.game_id,
                            expected_number,
                            number,
                        );
                    }
                    self.inning_number = *number;

                    if *batting_team_name != self.batting_team().team_name {
                        warn!(
                            "Batting team name from InningStart ({batting_team_name}) did \
                            not match the one from LiveNow ({})",
                            self.batting_team().team_name,
                        );
                    }
                    if *batting_team_emoji != self.batting_team().team_emoji {
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
                [ParsedEventMessageDiscriminants::MoundVisit]
                ParsedEventMessage::MoundVisit { emoji, team } => {
                    if *team != self.defending_team().team_name {
                        warn!(
                            "Batting team name from MoundVisit ({team}) did \
                            not match the one from LiveNow ({})",
                            self.defending_team().team_name,
                        );
                    }
                    if *emoji != self.defending_team().team_emoji {
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
               [ParsedEventMessageDiscriminants::NowBatting]
               // TODO Handle stats
               ParsedEventMessage::NowBatting { batter: batter_name, stats: _ } => {
                   self.increment_batter_count();
                   self.check_batter(batter_name, event.discriminant());

                   self.phase = GamePhase::ExpectPitch;
                   None
               },
               [ParsedEventMessageDiscriminants::MoundVisit]
               // TODO handle every single member of this variant
               ParsedEventMessage::MoundVisit { .. } => {
                   self.phase = GamePhase::ExpectMoundVisitOutcome;
                   None
               },
            ),
            GamePhase::ExpectPitch => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Ball]
                // TODO handle every single member of this variant
                ParsedEventMessage::Ball { count, .. } => {
                    self.count_balls += 1;
                    self.check_count(*count);

                    self.detail_builder(index)
                        .build_some(TaxaEventType::Ball)
                },
                [ParsedEventMessageDiscriminants::Strike]
                // TODO handle every single member of this variant
                ParsedEventMessage::Strike { strike, count, .. } => {
                    self.count_strikes += 1;
                    self.check_count(*count);

                    self.detail_builder(index)
                        .build_some(match strike {
                            StrikeType::Looking => { TaxaEventType::StrikeLooking }
                            StrikeType::Swinging => { TaxaEventType::StrikeSwinging }
                        })
                },
                [ParsedEventMessageDiscriminants::StrikeOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::StrikeOut { strike, .. } => {
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
                [ParsedEventMessageDiscriminants::Foul]
                // TODO handle every single member of this variant
                ParsedEventMessage::Foul { foul, count, .. } => {
                    // Falsehoods...
                    if !(*foul == FoulType::Ball && self.count_strikes >= 2) {
                        self.count_strikes += 1;
                    }
                    self.check_count(*count);

                    self.detail_builder(index)
                        .build_some(match foul {
                            FoulType::Tip => TaxaEventType::FoulTip,
                            FoulType::Ball => TaxaEventType::FoulBall,
                        })
                },
                [ParsedEventMessageDiscriminants::FairBall]
                ParsedEventMessage::FairBall { batter, hit, destination } => {
                    self.check_batter(batter, event.discriminant());

                    self.phase = GamePhase::ExpectFairBallOutcome(FairBall {
                        index,
                        hit_type: *hit,
                        hit_destination: *destination,
                    });
                    None
                },
                [ParsedEventMessageDiscriminants::Walk]
                // TODO handle every single member of this variant
                ParsedEventMessage::Walk { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();

                    self.detail_builder(index)
                        .build_some(TaxaEventType::Walk)
                },
                [ParsedEventMessageDiscriminants::HitByPitch]
                // TODO handle every single member of this variant
                ParsedEventMessage::HitByPitch { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();

                    self.detail_builder(index)
                        .build_some(TaxaEventType::HitByPitch)
                },
            ),
            GamePhase::ExpectFairBallOutcome(fair_ball) => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::CaughtOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::CaughtOut { batter, catcher, hit, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();
                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .fielder(*catcher)
                        .build_some(TaxaEventType::CaughtOut)
                },
                [ParsedEventMessageDiscriminants::GroundedOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::GroundedOut { batter, fielders, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();
                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .fielders(fielders.clone())
                        .build_some(TaxaEventType::GroundedOut)
                },
                [ParsedEventMessageDiscriminants::BatterToBase]
                // TODO handle every single member of this variant
                ParsedEventMessage::BatterToBase { batter, distance, fielder, advances, ..  } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();

                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .hit_type((*distance).into())
                        .fielder(*fielder)
                        .advances(advances.clone())
                        .build_some(TaxaEventType::Hit)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldingError]
                // TODO handle every single member of this variant
                ParsedEventMessage::ReachOnFieldingError { batter, fielder, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();

                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .fielder(*fielder)
                        .build_some(TaxaEventType::FieldingError)
                },
                [ParsedEventMessageDiscriminants::HomeRun]
                // TODO handle every single member of this variant
                ParsedEventMessage::HomeRun { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();

                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .build_some(TaxaEventType::HomeRun)
                },
                [ParsedEventMessageDiscriminants::DoublePlayCaught]
                // TODO handle every single member of this variant
                ParsedEventMessage::DoublePlayCaught { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();
                    self.add_out(); // It's a double play -- we add two outs

                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .build_some(TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::DoublePlayGrounded]
                // TODO handle every single member of this variant
                ParsedEventMessage::DoublePlayGrounded { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();
                    self.add_out(); // It's a double play -- we add two outs

                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .build_some(TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::ForceOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::ForceOut { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();

                    self.detail_builder(index)
                        .fair_ball(fair_ball)
                        .build_some(TaxaEventType::ForceOut)
                },
            ),
            GamePhase::ExpectInningEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningEnd]
                ParsedEventMessage::InningEnd { number, side } => {
                    if *number != self.inning_number {
                        warn!("Unexpected inning number in {}: expected {}, but saw {number}", self.game_id, self.inning_number);
                    }

                    if *side != self.inning_half {
                        warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, self.inning_half);
                    }

                    // TODO Implement the full extra innings rule
                    if *number == 9 {
                        self.phase = GamePhase::ExpectGameEnd;
                    } else {
                        self.phase = GamePhase::ExpectInningStart;
                    }
                    None
                },
            ),
            GamePhase::ExpectMoundVisitOutcome => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::PitcherRemains]
                // TODO handle every single member of this variant
                ParsedEventMessage::PitcherRemains { .. } => {
                    // I think this is not always ExpectNowBatting. I may have
                    // to store the state-to-return-to as a data member of
                    // GamePhase::ExpectMoundVisitOutcome
                    self.phase = GamePhase::ExpectNowBatting;
                    None
                },
                [ParsedEventMessageDiscriminants::PitcherSwap]
                // TODO handle every single member of this variant
                ParsedEventMessage::PitcherSwap { .. } => {
                    // I think this is not always ExpectNowBatting. I may have
                    // to store the state-to-return-to as a data member of
                    // GamePhase::ExpectMoundVisitOutcome
                    self.phase = GamePhase::ExpectNowBatting;
                    None
                },
            ),
            GamePhase::ExpectGameEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::PitcherSwap]
                ParsedEventMessage::GameOver => {
                    self.phase = GamePhase::ExpectFinalScore;
                    None
                },
            ),
            GamePhase::ExpectFinalScore => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Recordkeeping]
                // TODO handle every single member of this variant
                ParsedEventMessage::Recordkeeping { .. } => {
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

fn positioned_player_as_ref<StrT: AsRef<str>>(
    p: &PositionedPlayer<StrT>,
) -> PositionedPlayer<&str> {
    PositionedPlayer {
        name: p.name.as_ref(),
        position: p.position,
    }
}

fn runner_advance_as_ref<StrT: AsRef<str>>(
    a: &RunnerAdvance<StrT>,
) -> RunnerAdvance<&str> {
    RunnerAdvance {
        runner: a.runner.as_ref(),
        base: a.base,
    }
}

impl<StrT: AsRef<str>> EventDetail<StrT> {
    fn count(&self) -> (u8, u8) {
        (self.count_balls, self.count_strikes)
    }
    
    fn fielders_iter(&self) -> impl Iterator<Item = PositionedPlayer<&str>> {
        self
            .fielders
            .iter()
            .map(positioned_player_as_ref)
    }
    
    fn fielders(&self) -> Vec<PositionedPlayer<&str>> {
        self.fielders_iter().collect()
    }
    
    fn advances_iter(&self) -> impl Iterator<Item = RunnerAdvance<&str>> {
        self
            .advances
            .iter()
            .map(runner_advance_as_ref)
    }
    
    fn advances(&self) -> Vec<RunnerAdvance<&str>> {
        self.advances_iter().collect()
    }

    pub fn to_parsed(&self) -> ParsedEventMessage<&str> {
        match self.detail_type {
            TaxaEventType::Ball => ParsedEventMessage::Ball {
                steals: vec![],
                count: self.count(),
            },
            TaxaEventType::StrikeLooking => {
                if self.outs_after > self.outs_before {
                    ParsedEventMessage::StrikeOut {
                        foul: None,
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Looking,
                        steals: vec![],
                    }
                } else {
                    ParsedEventMessage::Strike {
                        strike: StrikeType::Looking,
                        steals: vec![],
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::StrikeSwinging => {
                if self.outs_after > self.outs_before {
                    ParsedEventMessage::StrikeOut {
                        foul: None,
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Swinging,
                        steals: vec![],
                    }
                } else {
                    ParsedEventMessage::Strike {
                        strike: StrikeType::Swinging,
                        steals: vec![],
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::FoulTip => ParsedEventMessage::Foul {
                foul: FoulType::Tip,
                steals: vec![],
                count: self.count(),
            },
            TaxaEventType::FoulBall => ParsedEventMessage::Foul {
                foul: FoulType::Ball,
                steals: vec![],
                count: self.count(),
            },
            TaxaEventType::Hit => {
                let (fielder,) = self
                    .fielders_iter()
                    .collect_tuple()
                    .expect("Hit must have exactly one fielder. TODO Handle this properly.");

                ParsedEventMessage::BatterToBase {
                    batter: self.batter_name.as_ref(),
                    distance: match self.hit_type {
                        None => {
                            panic!("EventDetail with the Hit detail_type must have a hit_type")
                        }
                        Some(TaxaHitType::Single) => Distance::Single,
                        Some(TaxaHitType::Double) => Distance::Double,
                        Some(TaxaHitType::Triple) => Distance::Triple,
                    },
                    hit: self.fair_ball_type
                        .expect("BatterToBase type must have a fair_ball_type")
                        .into(),
                    fielder,
                    scores: vec![],
                    advances: self.advances(),
                }
            }
            TaxaEventType::ForceOut => {
                ParsedEventMessage::ForceOut {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    hit: HitType::GroundBall, // TODO
                    out: todo!(),
                    scores: vec![],
                    advances: vec![],
                }
            }
            TaxaEventType::CaughtOut => {
                let (catcher,) = self
                    .fielders_iter()
                    .collect_tuple()
                    .expect("CaughtOut must have exactly one fielder. TODO Handle this properly.");

                ParsedEventMessage::CaughtOut {
                    batter: self.batter_name.as_ref(),
                    hit: self
                        .fair_ball_type
                        .expect("CaughtOut type must have a fair_ball_type")
                        .into(),
                    catcher,
                    scores: vec![],
                    advances: vec![],
                    sacrifice: false, // TODO
                    perfect: false,   // TODO
                }
            }
            TaxaEventType::GroundedOut => ParsedEventMessage::GroundedOut {
                batter: self.batter_name.as_ref(),
                fielders: self.fielders(),
                scores: vec![],
                advances: vec![],
            },
            TaxaEventType::Walk => ParsedEventMessage::Walk {
                batter: self.batter_name.as_ref(),
                scores: vec![],
                advances: vec![],
            },
            TaxaEventType::HomeRun => {
                ParsedEventMessage::HomeRun {
                    batter: self.batter_name.as_ref(),
                    hit: self.fair_ball_type
                        .expect("HomeRun type must have a fair_ball_type")
                        .into(),
                    destination: self.fair_ball_direction
                        .expect("HomeRun type must have a fair_ball_direction")
                        .into(),
                    scores: vec![],
                }
            }
            TaxaEventType::FieldingError => {
                let (fielder,) = self
                    .fielders_iter()
                    .collect_tuple()
                    .expect("FieldingError must have exactly one fielder. TODO Handle this properly.");

                ParsedEventMessage::ReachOnFieldingError {
                    batter: self.batter_name.as_ref(),
                    fielder,
                    error: FieldingErrorType::Throwing, // TODO
                    scores: vec![],
                    advances: vec![],
                }
            }
            TaxaEventType::HitByPitch => ParsedEventMessage::HitByPitch {
                batter: self.batter_name.as_ref(),
                scores: vec![],
                advances: vec![],
            },
            TaxaEventType::DoublePlay => {
                ParsedEventMessage::DoublePlayCaught {
                    batter: self.batter_name.as_ref(),
                    hit: HitType::Popup, // TODO
                    fielders: self.fielders(),
                    play: todo!(),
                    scores: vec![],
                    advances: vec![],
                }
            }
        }
    }

    pub fn to_parsed_contact(&self) -> ParsedEventMessage<&str> {
        // We're going to construct a FairBall for this no matter
        // whether we had the type.
        ParsedEventMessage::FairBall {
            batter: self.batter_name.as_ref(),
            hit: self.fair_ball_type
                .expect("Event with a fair_ball_index must have a fair_ball_type")
                .into(),
            destination: self.fair_ball_direction
                .expect("Event with a fair_ball_index must have a fair_ball_direction")
                .into(),
        }
    }
}
