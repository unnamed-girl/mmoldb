use crate::db::{
    TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat, TaxaEventType,
    TaxaFairBallType, TaxaFieldingErrorType, TaxaHitType, TaxaPosition,
};
use itertools::{EitherOrBoth, Itertools, PeekingNext};
use log::{info, warn};
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::{Base, BaseNameVariant, BatterStat, Distance, FairBallDestination, FairBallType, FieldingErrorType, FoulType, HomeAway, NowBattingStats, StrikeType, TopBottom};
use mmolb_parsing::parsed_event::{BaseSteal, FieldingAttempt, ParsedEventMessageDiscriminants, PositionedPlayer, RunnerAdvance, RunnerOut};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Write;
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
pub struct EventDetailRunner<StrT> {
    pub name: StrT,
    pub base_before: Option<TaxaBase>,
    pub base_after: TaxaBase,
    pub is_out: bool,
    pub base_description_format: Option<TaxaBaseDescriptionFormat>,
    pub is_steal: bool,
}

#[derive(Debug)]
pub struct EventDetailFielder<StrT> {
    pub name: StrT,
    pub position: TaxaPosition,
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
    pub fielders: Vec<EventDetailFielder<StrT>>,

    pub detail_type: TaxaEventType,
    pub hit_type: Option<TaxaHitType>,
    pub fair_ball_type: Option<TaxaFairBallType>,
    pub fair_ball_direction: Option<TaxaPosition>,
    pub fielding_error_type: Option<TaxaFieldingErrorType>,

    pub baserunners: Vec<EventDetailRunner<StrT>>,
}

#[derive(Debug, Copy, Clone)]
struct FairBall {
    index: usize,
    fair_ball_type: FairBallType,
    fair_ball_destination: FairBallDestination,
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
pub struct BatterStats {
    hits: u8,
    at_bats: u8,
    stats: Vec<()>,
}

impl BatterStats {
    pub fn is_empty(&self) -> bool {
        self.stats.is_empty() && self.hits == 0 && self.at_bats == 0
    }
}

#[derive(Debug)]
pub struct BatterInGame<StrT> {
    name: StrT,
    stats: BatterStats,
}

impl<'g> BatterInGame<&'g str> {
    pub fn from_position_player(player: &PositionedPlayer<&'g str>) -> Self {
        Self {
            name: player.name,
            stats: BatterStats {
                hits: 0,
                at_bats: 0,
                stats: Vec::new(),
            },
        }
    }
}

#[derive(Debug)]
pub struct TeamInGame<'g> {
    team_name: &'g str,
    team_emoji: &'g str,
    pitcher_name: &'g str,
    lineup: Vec<BatterInGame<&'g str>>,
    // This is incremented when a PA finishes, so in between PAs (and before the first PA) it
    // represents the next batter
    batter_count: usize,
}

#[derive(Debug, Clone)]
struct RunnerOn<'g> {
    runner_name: &'g str,
    base: TaxaBase,
}

#[derive(Debug, Clone)]
struct GameState<'g> {
    prev_event_type: ParsedEventMessageDiscriminants,
    phase: GamePhase,
    home_score: u8,
    away_score: u8,
    inning_number: u8,
    inning_half: TopBottom,
    count_balls: u8,
    count_strikes: u8,
    outs: i32,
    runners_on: VecDeque<RunnerOn<'g>>,
}

#[derive(Debug)]
pub struct Game<'g> {
    // Should never change
    game_id: &'g str,

    // Aggregates
    away: TeamInGame<'g>,
    home: TeamInGame<'g>,

    // Changes all the time
    state: GameState<'g>,
}

#[derive(Debug)]
struct ParsedEventMessageIter<'g: 'a, 'a, IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>> {
    inner: &'a mut IterT,
    prev_event_type: Option<ParsedEventMessageDiscriminants>,
}

impl<'g: 'a, 'a, IterT> ParsedEventMessageIter<'g, 'a, IterT>
where
    IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>,
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
    ) -> Result<&'a ParsedEventMessage<&'g str>, SimError> {
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

fn is_matching_advance<'g>(prev_runner: &RunnerOn<'g>, advance: &RunnerAdvance<&'g str>) -> bool {
    if prev_runner.runner_name != advance.runner {
        // If it's not the same runner, no match
        false
    } else if !(prev_runner.base < advance.base.into()) {
        // If the base they advanced to isn't ahead of the base they started on, no match
        false
    } else {
        true
    }
}

fn is_matching_runner_out<'g>(prev_runner: &RunnerOn<'g>, out: &RunnerOut<&'g str>) -> bool {
    if prev_runner.runner_name != out.runner {
        // If it's not the same runner, no match
        false
    } else if !(prev_runner.base <= out.base.into()) && out.base != BaseNameVariant::Home {
        // If the base they got out at to is behind the base they started on, no match
        false
    } else {
        true
    }
}

fn is_matching_steal<'g>(prev_runner: &RunnerOn<'g>, steal: &BaseSteal<&'g str>) -> bool {
    if prev_runner.runner_name != steal.runner {
        // If it's not the same runner, no match
        info!(
            "SB: {} doesn't match {}",
            prev_runner.runner_name, steal.runner
        );
        false
    } else if !(prev_runner.base < steal.base.into()) && steal.base != Base::Home {
        // If the base they advanced to isn't ahead of the base they started on, no match
        // This could be restricted to the very next base but I don't think that's necessary
        info!("SB: Can't steal {} from {}", steal.base, prev_runner.base);
        false
    } else {
        info!(
            "SB: {} stole {} from {}",
            prev_runner.runner_name, steal.base, prev_runner.base
        );
        true
    }
}

struct EventDetailBuilder<'g> {
    prev_game_state: GameState<'g>,
    game_event_index: usize,
    batter_count_at_event_start: usize,
    fair_ball_event_index: Option<usize>,
    fair_ball_type: Option<TaxaFairBallType>,
    fair_ball_direction: Option<TaxaPosition>,
    hit_type: Option<TaxaHitType>,
    fielding_error_type: Option<TaxaFieldingErrorType>,
    fielders: Vec<PositionedPlayer<&'g str>>,
    advances: Vec<RunnerAdvance<&'g str>>,
    scores: Vec<&'g str>,
    steals: Vec<BaseSteal<&'g str>>,
    runner_added: Option<(&'g str, TaxaBase)>,
    runners_out: Vec<RunnerOut<&'g str>>,
}

impl<'g> EventDetailBuilder<'g> {
    fn fair_ball(mut self, fair_ball: FairBall) -> Self {
        self.fair_ball_event_index = Some(fair_ball.index);
        self.fair_ball_type = Some(fair_ball.fair_ball_type.into());
        self.fair_ball_direction = Some(fair_ball.fair_ball_destination.into());
        self
    }

    fn hit_type(mut self, hit_type: TaxaHitType) -> Self {
        self.hit_type = Some(hit_type);
        self
    }

    fn fielding_error_type(mut self, fielding_error_type: TaxaFieldingErrorType) -> Self {
        self.fielding_error_type = Some(fielding_error_type);
        self
    }

    fn fielder(mut self, fielder: PositionedPlayer<&'g str>) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = vec![fielder];
        self
    }

    fn fielders(mut self, fielders: Vec<PositionedPlayer<&'g str>>) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = fielders;
        self
    }

    fn runner_changes(
        mut self,
        advances: Vec<RunnerAdvance<&'g str>>,
        scores: Vec<&'g str>,
    ) -> Self {
        if !self.advances.is_empty() {
            warn!("EventDetailBuilder overwrote existing advances");
        }

        if !self.scores.is_empty() {
            warn!("EventDetailBuilder overwrote existing scores");
        }

        if !self.steals.is_empty() {
            warn!("Called runner_changes() and steals() on the same EventDetailBuilder");
        }

        self.advances = advances;
        self.scores = scores;
        self
    }

    fn steals(mut self, steals: Vec<BaseSteal<&'g str>>) -> Self {
        if !self.advances.is_empty() {
            warn!("Called runner_changes() and steals() on the same EventDetailBuilder");
        }

        if !self.scores.is_empty() {
            warn!("Called runner_changes() and steals() on the same EventDetailBuilder");
        }

        if !self.steals.is_empty() {
            warn!("EventDetailBuilder overwrote existing steals");
        }

        self.steals = steals;
        self
    }

    fn add_runner(mut self, runner_name: &'g str, to_base: TaxaBase) -> Self {
        self.runner_added = Some((runner_name, to_base));
        self
    }

    fn add_out(mut self, runner_out: RunnerOut<&'g str>) -> Self {
        self.runners_out.push(runner_out);
        self
    }

    pub fn build_some(
        self,
        game: &Game<'g>,
        type_detail: TaxaEventType,
    ) -> Option<EventDetail<&'g str>> {
        Some(self.build(game, type_detail))
    }

    pub fn build(self, game: &Game<'g>, type_detail: TaxaEventType) -> EventDetail<&'g str> {
        let mut runner_state = "Building event with previous runners:".to_string();
        for runner in &self.prev_game_state.runners_on {
            write!(
                runner_state,
                "\n    - {} on {}",
                runner.runner_name, runner.base
            )
            .unwrap();
        }
        write!(runner_state, "\nsteals:").unwrap();
        for steal in &self.steals {
            if steal.caught {
                write!(
                    runner_state,
                    "\n    - {} tried to steal {}",
                    steal.runner, steal.base
                )
                .unwrap();
            } else {
                write!(
                    runner_state,
                    "\n    - {} stole {}",
                    steal.runner, steal.base
                )
                .unwrap();
            }
        }
        write!(runner_state, "\nscores:").unwrap();
        for score in &self.scores {
            write!(runner_state, "\n    - {score}").unwrap();
        }
        write!(runner_state, "\nadvances:").unwrap();
        for advance in &self.advances {
            write!(
                runner_state,
                "\n    - {} to {}",
                advance.runner, advance.base
            )
            .unwrap();
        }
        write!(runner_state, "\nrunners out:").unwrap();
        for out in &self.runners_out {
            write!(runner_state, "\n    - {} out at {}", out.runner, out.base).unwrap();
        }
        write!(runner_state, "\nrunners added:").unwrap();
        if let Some((name, base)) = self.runner_added {
            write!(runner_state, "\n    - {} on {}", name, base).unwrap();
        }
        info!("{}", runner_state);

        let batter_name = game
            .batter_for_active_team(self.batter_count_at_event_start)
            .name;

        let mut scores = self.scores.into_iter();
        let mut advances = self.advances.into_iter().peekable();
        let mut runners_out = self.runners_out.into_iter().peekable();
        let mut steals = self.steals.into_iter().peekable();

        let runners_out_ref = &mut runners_out;
        let mut baserunners: Vec<_> = self
            .prev_game_state
            .runners_on
            .into_iter()
            .map(|prev_runner| {
                if let Some(steal) = steals.peeking_next(|s| is_matching_steal(&prev_runner, s)) {
                    // There can't be steals and scorers in the same event so we're safe to do this first
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: steal.base.into(),
                        is_out: steal.caught,
                        base_description_format: None,
                        is_steal: true,
                    }
                } else if let Some(scorer_name) = scores.next() {
                    // First: If there are any scores left, they MUST be in runner order. No need
                    // to search for a match.
                    if scorer_name != prev_runner.runner_name {
                        panic!("A runner who was not at the front of the runners list scored!")
                    }
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: TaxaBase::Home,
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }
                } else if let Some(advance) =
                    advances.peeking_next(|a| is_matching_advance(&prev_runner, a))
                {
                    // If the runner didn't score, they may have advanced
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: advance.base.into(),
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }
                } else if let Some(out) =
                    runners_out_ref.peeking_next(|o| is_matching_runner_out(&prev_runner, o))
                {
                    // If the runner didn't score or advance, they may have gotten out
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: out.base.into(),
                        is_out: true,
                        base_description_format: Some(out.base.into()),
                        is_steal: false,
                    }
                } else {
                    // If the runner didn't score, advance, or get out they just stayed on base
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: prev_runner.base,
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }
                }
            })
            .chain(
                // Add runners who made it to base this event
                self.runner_added
                    .into_iter()
                    .map(|(name, base)| EventDetailRunner {
                        name,
                        base_before: None,
                        base_after: base,
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }),
            )
            .collect();

        // Semantically I want this to be a .chain(), but it has to be .extend() for lifetime
        // reasons
        baserunners.extend(
            // The remaining runners out should be batter-runners who got out this event, but
            // who have a baserunner entry because we need to record which base they got out
            // at. This should be mutually exclusive with runners_added, so their relative
            // order doesn't matter.
            runners_out_ref.map(|out| {
                if out.runner != batter_name {
                    warn!(
                        "Got a batter-runner entry in `baserunners` that has the wrong name \
                            ({}, expected {batter_name})",
                        out.runner,
                    );
                }

                EventDetailRunner {
                    name: out.runner,
                    base_before: None,
                    base_after: out.base.into(),
                    is_out: true,
                    base_description_format: Some(out.base.into()),
                    is_steal: false,
                }
            }),
        );

        assert!(
            scores.next().is_none(),
            "At least one scoring runner was not found!"
        );
        assert!(
            advances.next().is_none(),
            "At least one advancing runner was not found!"
        );
        assert!(
            runners_out.next().is_none(),
            "At least one runner out was not found!"
        );
        assert!(
            steals.next().is_none(),
            "At least one stealing runner was not found!"
        );

        let fielders = self
            .fielders
            .iter()
            .map(|f| EventDetailFielder {
                name: f.name,
                position: f.position.into(),
            })
            .collect();

        EventDetail {
            game_id: game.game_id,
            game_event_index: self.game_event_index,
            fair_ball_event_index: self.fair_ball_event_index,
            inning: game.state.inning_number,
            top_of_inning: game.state.inning_half.is_top(),
            count_balls: game.state.count_balls,
            count_strikes: game.state.count_strikes,
            outs_before: self.prev_game_state.outs,
            outs_after: game.state.outs,
            batter_count: self.batter_count_at_event_start,
            batter_name,
            pitcher_name: game.defending_team().pitcher_name,
            fielders,
            detail_type: type_detail,
            hit_type: self.hit_type,
            fair_ball_type: self.fair_ball_type,
            fair_ball_direction: self.fair_ball_direction,
            fielding_error_type: self.fielding_error_type,
            baserunners,
        }
    }
}

impl<'g> Game<'g> {
    pub fn new<'a, IterT>(game_id: &'g str, events: &'a mut IterT) -> Result<Game<'g>, SimError>
    where
        'g: 'a,
        IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>,
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
                lineup: away_lineup
                    .into_iter()
                    .map(BatterInGame::from_position_player)
                    .collect(),
                batter_count: 0,
            },
            home: TeamInGame {
                team_name: home_team_name,
                team_emoji: home_team_emoji,
                pitcher_name: home_pitcher_name,
                lineup: home_lineup
                    .iter()
                    .map(BatterInGame::from_position_player)
                    .collect(),
                batter_count: 0,
            },
            state: GameState {
                prev_event_type: ParsedEventMessageDiscriminants::PlayBall,
                phase: GamePhase::ExpectInningStart,
                home_score: 0,
                away_score: 0,
                inning_number: 0,
                inning_half: TopBottom::Bottom,
                count_balls: 0,
                count_strikes: 0,
                outs: 0,
                runners_on: Default::default(),
            },
        })
    }

    fn batting_team(&self) -> &TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &self.away,
            TopBottom::Bottom => &self.home,
        }
    }

    fn batting_team_mut(&mut self) -> &mut TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &mut self.away,
            TopBottom::Bottom => &mut self.home,
        }
    }

    fn defending_team(&self) -> &TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &self.home,
            TopBottom::Bottom => &self.away,
        }
    }

    fn check_count(&self, (balls, strikes): (u8, u8)) {
        if self.state.count_balls != balls {
            warn!(
                "Unexpected number of balls in {}: expected {}, but saw {balls}",
                self.game_id, self.state.count_balls
            );
        }
        if self.state.count_strikes != strikes {
            warn!(
                "Unexpected number of strikes in {}: expected {}, but saw {strikes}",
                self.game_id, self.state.count_strikes
            );
        }
    }

    fn active_batter(&self) -> &BatterInGame<&'g str> {
        let batting_team = self.batting_team();
        let lineup = &batting_team.lineup;
        &lineup[batting_team.batter_count % lineup.len()]
    }

    fn active_batter_mut(&mut self) -> &mut BatterInGame<&'g str> {
        let batting_team = self.batting_team_mut();
        let lineup = &mut batting_team.lineup;
        let lineup_len = lineup.len();
        &mut lineup[batting_team.batter_count % lineup_len]
    }

    fn batter_for_active_team(&self, batter_count: usize) -> &BatterInGame<&'g str> {
        let batting_team = self.batting_team();
        let lineup = &batting_team.lineup;
        &lineup[batter_count % lineup.len()]
    }

    fn check_batter(&self, batter_name: &str, event_type: ParsedEventMessageDiscriminants) {
        let active_batter = self.active_batter();
        if active_batter.name != batter_name {
            warn!(
                "Unexpected batter name in {:#?}: Expected {}, but saw {}",
                event_type, active_batter.name, batter_name,
            );
        }
    }

    fn check_fair_ball_type(
        &self,
        fair_ball_from_previous_event: &FairBall,
        fair_ball_type_from_this_event: FairBallType,
        event_type: ParsedEventMessageDiscriminants,
    ) {
        if fair_ball_from_previous_event.fair_ball_type != fair_ball_type_from_this_event {
            warn!(
                "Mismatched fair ball type in {event_type:#?}: expected {} but saw {}",
                fair_ball_from_previous_event.fair_ball_type, fair_ball_type_from_this_event,
            );
        }
    }

    fn check_fair_ball_destination(
        &self,
        fair_ball_from_previous_event: &FairBall,
        fair_ball_destination_from_this_event: FairBallDestination,
        event_type: ParsedEventMessageDiscriminants,
    ) {
        if fair_ball_from_previous_event.fair_ball_destination
            != fair_ball_destination_from_this_event
        {
            warn!(
                "Mismatched fair ball destination in {event_type:#?}: expected {} but saw {}",
                fair_ball_from_previous_event.fair_ball_destination,
                fair_ball_destination_from_this_event,
            );
        }
    }

    fn detail_builder(
        &self,
        prev_game_state: GameState<'g>,
        game_event_index: usize,
        batter_count: usize,
    ) -> EventDetailBuilder<'g> {
        EventDetailBuilder {
            prev_game_state,
            batter_count_at_event_start: batter_count,
            fair_ball_event_index: None,
            game_event_index,
            fielders: Vec::new(),
            advances: Vec::new(),
            hit_type: None,
            fair_ball_type: None,
            fair_ball_direction: None,
            fielding_error_type: None,
            scores: Vec::new(),
            steals: Vec::new(),
            runner_added: None,
            runners_out: Vec::new(),
        }
    }

    // Note: Must happen after all outs for this event are added
    pub fn finish_pa(&mut self) {
        // Occam's razor: assume "at bats" is actually PAs until proven
        // otherwise
        self.active_batter_mut().stats.at_bats += 1;

        self.state.count_strikes = 0;
        self.state.count_balls = 0;

        if self.state.outs >= 3 {
            self.state.phase = GamePhase::ExpectInningEnd;
        } else {
            self.state.phase = GamePhase::ExpectNowBatting;
        }

        self.batting_team_mut().batter_count += 1;
    }

    pub fn add_out(&mut self) {
        self.state.outs += 1;

        // This is usually redundant with finish_pa, but not in the case of inning-ending
        // caught stealing
        if self.state.outs >= 3 {
            self.state.phase = GamePhase::ExpectInningEnd;
        }
    }

    pub fn runner_out(&mut self, out: &RunnerOut<&'g str>) {
        self.check_internal_baserunner_consistency();

        // Assume runner_out is called after all actual advances have
        // been processed, so we know that bases that can be emptied
        // will be emptied.
        let candidates = self.get_advance_candidates(out.runner, out.base.into());

        if candidates.is_empty() {
            panic!("Tried to put a runner out but there are no matching runners");
        } else {
            if candidates.len() > 1 {
                warn!(
                    "Multiple candidates for a runner out. Taking the one who's farthest on the \
                    basepaths."
                );
            }
            self.state
                .runners_on
                .remove(candidates[0])
                .expect("Tried to remove a runner who did not exist");
        }
        self.add_out();

        self.check_internal_baserunner_consistency();
    }

    pub fn batter_or_runner_out(&mut self, out: &RunnerOut<&'g str>) {
        // For now, assume the batter is always out at first. I'll
        // change this if it turns out not to be true.
        if out.runner == self.active_batter().name && Base::First == out.base.into() {
            // Then assume this is the runner and all that's needed is to add an out
            self.add_out();
        } else {
            self.runner_out(out);
        }
    }

    pub fn add_runner(&mut self, runner_name: &'g str, base: TaxaBase) {
        self.state
            .runners_on
            .push_back(RunnerOn { runner_name, base })
    }

    pub fn add_runner_and_force_advance(&mut self, runner_name: &'g str, base: TaxaBase) {
        let mut base_to_clear = base;
        for runner in self.state.runners_on.iter_mut().rev() {
            if runner.base == base {
                base_to_clear = base_to_clear.next_base();
                runner.base = base_to_clear;
            } else {
                // As soon as one runner doesn't need advancing, no subsequent runners do
                break;
            }
        }

        self.add_runner(runner_name, base)
    }

    fn check_baserunner_consistency(&self, raw_event: &mmolb_parsing::game::Event) {
        self.check_internal_baserunner_consistency();

        let mut on_1b = false;
        let mut on_2b = false;
        let mut on_3b = false;

        for runner in &self.state.runners_on {
            match runner.base {
                TaxaBase::Home => {}
                TaxaBase::First => on_1b = true,
                TaxaBase::Second => on_2b = true,
                TaxaBase::Third => on_3b = true,
            }
        }

        assert_eq!(on_1b, raw_event.on_1b);
        assert_eq!(on_2b, raw_event.on_2b);
        assert_eq!(on_3b, raw_event.on_3b);
    }

    fn check_internal_baserunner_consistency(&self) {
        assert!(
            self.state
                .runners_on
                .iter()
                .is_sorted_by(|a, b| a.base > b.base),
            "Baserunners list must always be sorted descending by base",
        );

        assert_eq!(
            self.state.runners_on.len(),
            self.state.runners_on.iter().unique_by(|r| r.base).count(),
            "Baserunners list must not have two runners on the same base",
        );
    }

    #[allow(non_snake_case)]
    fn do_scores__dont_call_directly(&mut self, scores: &[&'g str]) {
        self.add_runs_to_batting_team(scores.len() as u8);

        // Scores is easy, runners ALWAYS score in order
        for &scorer in scores {
            match self.state.runners_on.pop_front() {
                None => panic!("Tried to score a runner who didn't exist"),
                Some(s) if s.runner_name == scorer => (),
                Some(s) => panic!(
                    "Tried to score {scorer}, but {} was ahead of them on the bases",
                    s.runner_name
                ),
            }
        }
    }

    fn add_runs_to_batting_team(&mut self, runs: u8) {
        match self.state.inning_half {
            TopBottom::Top => {
                self.state.away_score += runs;
            }
            TopBottom::Bottom => {
                self.state.home_score += runs;
            }
        }
    }

    #[allow(non_snake_case)]
    fn do_advances__dont_call_directly(&mut self, advances: &[RunnerAdvance<&'g str>]) {
        self.check_internal_baserunner_consistency();
        // Be as robust to same-name runners as possible
        for advance in advances {
            let advancing_to_base = advance.base.into();
            let candidates = self.get_advance_candidates(advance.runner, advancing_to_base);

            if candidates.is_empty() {
                panic!("Tried to advance runner but there are no matching runners to advance")
            } else {
                if candidates.len() > 1 {
                    warn!(
                        "Multiple candidates for a baserunner advance. Taking the one who's \
                        farthest on the basepaths."
                    )
                }
                if advance.base == Base::Home {
                    warn!(
                        "Batter advanced to Home, which should be reported as a score rather than \
                        an advance."
                    );
                }
                self.state.runners_on[candidates[0]].base = advancing_to_base;
            }
        }
        self.check_internal_baserunner_consistency();
    }

    #[allow(non_snake_case)]
    fn do_steals__dont_call_directly(&mut self, steals: &[BaseSteal<&'g str>]) {
        self.check_internal_baserunner_consistency();
        // Be as robust to same-name runners as possible
        for steal in steals {
            let advancing_to_base = steal.base.into();
            let candidates = self.get_advance_candidates(steal.runner, advancing_to_base);

            if candidates.is_empty() {
                panic!(
                    "Tried to advance runner for stolen base but there are no matching runners to \
                    advance"
                )
            } else {
                if candidates.len() > 1 {
                    warn!(
                        "Multiple candidates for a baserunner advance for stolen base. Taking the \
                        one who's farthest on the basepaths."
                    )
                }
                if steal.caught || advancing_to_base == TaxaBase::Home {
                    self.state.runners_on.remove(candidates[0]);
                } else {
                    self.state.runners_on[candidates[0]].base = advancing_to_base;
                }
            }

            if steal.caught {
                self.add_out()
            } else if steal.base == Base::Home {
                self.add_runs_to_batting_team(1);
            }
        }
        self.check_internal_baserunner_consistency();
    }

    fn get_advance_candidates(&mut self, runner: &str, advancing_to_base: TaxaBase) -> Vec<usize> {
        // Get a list of candidate runners to advance, as indices
        // This is easier to do imperatively
        let mut candidates = Vec::new();
        let mut prev_occupied_base = None;
        for (runner_i, runner_on) in self.state.runners_on.iter().enumerate() {
            if runner_on.runner_name == runner {
                // This advance candidate is legal if prev_occupied_base is None
                // or if prev_occupied_base is
                let advance_is_legal = match prev_occupied_base {
                    // You can advance anywhere if there's no one ahead of you
                    None => true,
                    // The stored runners should never be at home, but if they are I suppose
                    // the interpretation should be that you can score
                    Some(TaxaBase::Home) => {
                        warn!("Player on home base in do_advances");
                        true
                    }
                    // You can advance if the previous runner is ahead of the base you
                    // are advancing to. Because home is base 0 this wouldn't match advances
                    // to home, but advances to home must have no batter ahead of you so they
                    // must match an earlier case.
                    Some(b) if b > advancing_to_base => true,
                    // You can't advance if none of the above are true
                    Some(_) => false,
                };

                if advance_is_legal {
                    candidates.push(runner_i);
                }
            }
            prev_occupied_base = Some(runner_on.base);
        }
        candidates
    }

    fn update_runners(
        &mut self,
        scores: &[&'g str],
        advances: &[RunnerAdvance<&'g str>],
        steals: &[BaseSteal<&'g str>],
    ) {
        // When these methods say don't call directly, this function is
        // the one they want you to call instead
        self.do_steals__dont_call_directly(steals);
        self.do_scores__dont_call_directly(scores);
        self.do_advances__dont_call_directly(advances);
    }

    pub fn next(
        &mut self,
        index: usize,
        event: &ParsedEventMessage<&'g str>,
        raw_event: &mmolb_parsing::game::Event,
    ) -> Result<Option<EventDetail<&'g str>>, SimError> {
        let previous_event = self.state.prev_event_type;
        let this_event_discriminant = event.discriminant();

        let detail_builder =
            self.detail_builder(self.state.clone(), index, self.batting_team().batter_count);

        let result = match self.state.phase {
            GamePhase::ExpectInningStart => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningStart]
                ParsedEventMessage::InningStart {
                    number,
                    side,
                    batting_team_emoji,
                    batting_team_name,
                    pitcher_status,
                    automatic_runner,
                } => {
                    if *side != self.state.inning_half.flip() {
                        warn!(
                            "Unexpected inning side in {}: expected {:?}, but saw {side:?}",
                            self.game_id,
                            self.state.inning_half.flip(),
                        );
                    }
                    self.state.inning_half = *side;

                    // If we just started a top, the number should increment
                    let expected_number = match self.state.inning_half {
                        TopBottom::Top => self.state.inning_number + 1,
                        TopBottom::Bottom => self.state.inning_number,
                    };

                    if *number != expected_number {
                        warn!(
                            "Unexpected inning number in {}: expected {}, but saw {}",
                            self.game_id,
                            expected_number,
                            number,
                        );
                    }
                    self.state.inning_number = *number;

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
                        self.state.inning_half,
                        self.state.inning_number,
                    );

                    // Add the automatic runner to our state without emitting a db event for it.
                    // This way they will just show up on base without having an event that put
                    // them there, which I think is the correct interpretation.
                    if let Some(runner_name) = automatic_runner {
                        self.state.runners_on.push_back(RunnerOn {
                            runner_name,
                            base: TaxaBase::Second, // Automatic runners are always placed on second
                        })
                    }

                    self.state.outs = 0;
                    self.state.phase = GamePhase::ExpectNowBatting;
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

                    self.state.phase = GamePhase::ExpectMoundVisitOutcome;
                    None
                },
            ),
            GamePhase::ExpectNowBatting => game_event!(
               (previous_event, event),
               [ParsedEventMessageDiscriminants::NowBatting]
               ParsedEventMessage::NowBatting { batter: batter_name, stats } => {
                   self.check_batter(batter_name, event.discriminant());
                   let batter = self.active_batter();
                   check_now_batting_stats(&stats, &batter.stats);

                   self.state.phase = GamePhase::ExpectPitch;
                   None
               },
               [ParsedEventMessageDiscriminants::MoundVisit]
               ParsedEventMessage::MoundVisit { emoji, team } => {
                   if self.defending_team().team_name != *team {
                        warn!(
                            "Team name in MoundVisit doesn't match: Expected {}, but saw {team}",
                            self.defending_team().team_name,
                        );
                   }

                   if self.defending_team().team_emoji != *emoji {
                        warn!(
                            "Team emoji in MoundVisit doesn't match: Expected {}, but saw {emoji}",
                            self.defending_team().team_emoji,
                        );
                   }

                   self.state.phase = GamePhase::ExpectMoundVisitOutcome;
                   None
               },
            ),
            GamePhase::ExpectPitch => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Ball]
                ParsedEventMessage::Ball { count, steals } => {
                    self.state.count_balls += 1;
                    self.check_count(*count);
                    self.update_runners(&[], &[], steals);

                    detail_builder
                        .steals(steals.clone())
                        .build_some(self, TaxaEventType::Ball)
                },
                [ParsedEventMessageDiscriminants::Strike]
                ParsedEventMessage::Strike { strike, count, steals } => {
                    self.state.count_strikes += 1;
                    self.check_count(*count);
                    self.update_runners(&[], &[], steals);

                    detail_builder
                        .steals(steals.clone())
                        .build_some(self, match strike {
                            StrikeType::Looking => { TaxaEventType::CalledStrike }
                            StrikeType::Swinging => { TaxaEventType::SwingingStrike }
                        })
                },
                [ParsedEventMessageDiscriminants::StrikeOut]
                ParsedEventMessage::StrikeOut { foul, batter, strike, steals } => {
                    self.check_batter(batter, event.discriminant());
                    if self.state.count_strikes < 2 {
                        warn!(
                            "Unexpected strikeout in {}: expected 2 strikes in the count, but \
                            there were {}",
                            self.game_id,
                            self.state.count_strikes,
                        );
                    }

                    self.update_runners(&[], &[], steals);
                    self.add_out();
                    self.finish_pa();

                    detail_builder
                        .steals(steals.clone())
                        .build_some(self, match (foul, strike) {
                            (None, StrikeType::Looking) => { TaxaEventType::CalledStrike }
                            (None, StrikeType::Swinging) => { TaxaEventType::SwingingStrike }
                            (Some(FoulType::Ball), _) => { panic!("Can't strike out on a foul ball") }
                            (Some(FoulType::Tip), StrikeType::Looking) => { panic!("Foul tip can't be a called strike") }
                            (Some(FoulType::Tip), StrikeType::Swinging) => { TaxaEventType::FoulTip }
                        })
                },
                [ParsedEventMessageDiscriminants::Foul]
                ParsedEventMessage::Foul { foul, steals, count } => {
                    // Falsehoods...
                    if !(*foul == FoulType::Ball && self.state.count_strikes >= 2) {
                        self.state.count_strikes += 1;
                    }
                    self.check_count(*count);
                    self.update_runners(&[], &[], steals);

                    detail_builder
                        .steals(steals.clone())
                        .build_some(self, match foul {
                            FoulType::Tip => TaxaEventType::FoulTip,
                            FoulType::Ball => TaxaEventType::FoulBall,
                        })
                },
                [ParsedEventMessageDiscriminants::FairBall]
                ParsedEventMessage::FairBall { batter, fair_ball_type, destination } => {
                    self.check_batter(batter, event.discriminant());

                    self.state.phase = GamePhase::ExpectFairBallOutcome(FairBall {
                        index,
                        fair_ball_type: *fair_ball_type,
                        fair_ball_destination: *destination,
                    });
                    None
                },
                [ParsedEventMessageDiscriminants::Walk]
                ParsedEventMessage::Walk { batter, advances, scores } => {
                    self.check_batter(batter, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_runner(batter, TaxaBase::First);
                    self.finish_pa();

                    detail_builder
                        .runner_changes(advances.clone(), scores.clone())
                        .add_runner(batter, TaxaBase::First)
                        .build_some(self, TaxaEventType::Walk)
                },
                [ParsedEventMessageDiscriminants::HitByPitch]
                ParsedEventMessage::HitByPitch { batter, advances, scores } => {
                    self.check_batter(batter, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_runner(batter, TaxaBase::First);
                    self.finish_pa();

                    detail_builder
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, TaxaEventType::HitByPitch)
                },
            ),
            GamePhase::ExpectFairBallOutcome(fair_ball) => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::CaughtOut]
                ParsedEventMessage::CaughtOut { batter, fair_ball_type, caught_by, advances, scores, sacrifice, perfect } => {
                    self.check_batter(batter, event.discriminant());
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_out();
                    self.finish_pa();

                    if *fair_ball_type != FairBallType::GroundBall {
                        if *sacrifice && scores.is_empty() {
                            warn!("Flyout was described as a sacrifice, but nobody scored");
                        } else if !*sacrifice && !scores.is_empty() {
                            warn!(
                                "Player(s) scored on flyout, but it was not described as a \
                                sacrifice",
                            );
                        }
                    } else if *sacrifice {
                        warn!("Non-flyout was described as sacrifice");
                    }

                    assert_eq!(*perfect, false, "TODO Handle perfect outs");

                    detail_builder
                        .fair_ball(fair_ball)
                        .fielder(*caught_by)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, TaxaEventType::CaughtOut)
                },
                [ParsedEventMessageDiscriminants::GroundedOut]
                ParsedEventMessage::GroundedOut { batter, fielders, scores, advances } => {
                    self.check_batter(batter, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_out();
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .fielders(fielders.clone())
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, TaxaEventType::GroundedOut)
                },
                [ParsedEventMessageDiscriminants::BatterToBase]
                ParsedEventMessage::BatterToBase { batter, distance, fair_ball_type, fielder, advances, scores } => {
                    self.check_batter(batter, event.discriminant());
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_runner(batter, (*distance).into());
                    self.finish_pa();

                    info!("BatterToBase with advances: {:?}", advances);

                    detail_builder
                        .fair_ball(fair_ball)
                        .hit_type((*distance).into())
                        .fielder(*fielder)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, TaxaEventType::Hit)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldingError]
                ParsedEventMessage::ReachOnFieldingError { batter, fielder, error, scores, advances } => {
                    self.check_batter(batter, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_runner(batter, TaxaBase::First);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .fielding_error_type((*error).into())
                        .fielder(*fielder)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, TaxaEventType::FieldingError)
                },
                [ParsedEventMessageDiscriminants::HomeRun]
                ParsedEventMessage::HomeRun { batter, fair_ball_type, destination, scores, grand_slam } => {
                    self.check_batter(batter, event.discriminant());
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, event.discriminant());
                    self.check_fair_ball_destination(&fair_ball, *destination, event.discriminant());

                    if *grand_slam && scores.len() != 3 {
                        warn!(
                            "Parsed a grand slam, but there were {} runners scored (expected 3)",
                            scores.len(),
                        );
                    } else if !*grand_slam && scores.len() == 3 {
                        warn!("There were 3 runners scored but we didn't parse a grand slam");
                    }

                    // This is the one situation where you can have
                    // scores but no advances, because after everyone
                    // scores there's no one left to advance
                    self.update_runners(scores, &[], &[]);
                    // Also the only situation where you have a score
                    // without the runner
                    self.add_runs_to_batting_team(1);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(Vec::new(), scores.clone())
                        .build_some(self, TaxaEventType::HomeRun)
                },
                [ParsedEventMessageDiscriminants::DoublePlayCaught]
                ParsedEventMessage::DoublePlayCaught { batter, advances, scores, out_two, fair_ball_type, fielders } => {
                    self.check_batter(batter, event.discriminant());
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.add_out(); // This is the out for the batter
                    self.runner_out(out_two);
                    self.finish_pa();  // Must be after all outs are added

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out_two)
                        .fielders(fielders.clone())
                        .build_some(self, TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::DoublePlayGrounded]
                // TODO handle every single member of this variant
                ParsedEventMessage::DoublePlayGrounded { batter, advances, scores, out_one, out_two, fielders, .. } => {
                    self.check_batter(batter, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.batter_or_runner_out(out_one);
                    self.batter_or_runner_out(out_two);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out_one)
                        .add_out(*out_two)
                        .fielders(fielders.clone())
                        .build_some(self, TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::ForceOut]
                ParsedEventMessage::ForceOut { batter, out, fielders, scores, advances, fair_ball_type } => {
                    self.check_batter(batter, event.discriminant());
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    self.runner_out(out);
                    self.add_runner_and_force_advance(batter, TaxaBase::First);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out)
                        .fielders(fielders.clone())
                        .build_some(self, TaxaEventType::ForceOut)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldersChoice]
                ParsedEventMessage::ReachOnFieldersChoice { batter, fielders, result, scores, advances } => {
                    self.check_batter(batter, event.discriminant());

                    self.update_runners(scores, advances, &[]);
                    if let FieldingAttempt::Out { out } = result {
                        self.runner_out(out);
                    }
                    self.add_runner_and_force_advance(batter, TaxaBase::First);
                    self.finish_pa();

                    match result {
                        FieldingAttempt::Out { out } => {
                            warn!("I've been wondering if this combination actually happens.");
                            detail_builder
                                .fair_ball(fair_ball)
                                .runner_changes(advances.clone(), scores.clone())
                                .add_out(*out)
                                .fielders(fielders.clone())
                                .build_some(self, TaxaEventType::FieldersChoice)
                        }
                        FieldingAttempt::Error { fielder, error } => {
                            match error {
                                FieldingErrorType::Throwing => {
                                    if let Some((listed_fielder,)) = fielders.iter().collect_tuple() {
                                        if listed_fielder.name != *fielder {
                                            warn!("Fielder who made the error ({}) is not the one listed as fielding the ball ({})", fielder, listed_fielder.name);
                                        }
                                    } else {
                                        warn!("Expected exactly one listed fielder in a fielder's choice with a throwing error");
                                    }
                                }
                                FieldingErrorType::Fielding => {
                                    todo!("What relationship does fielder and fielders have here?");
                                }
                            }

                            detail_builder
                                .fair_ball(fair_ball)
                                .runner_changes(advances.clone(), scores.clone())
                                .fielders(fielders.clone())
                                .fielding_error_type((*error).into())
                                .build_some(self, TaxaEventType::ErrorOnFieldersChoice)
                        }
                    }
                },
            ),
            GamePhase::ExpectInningEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningEnd]
                ParsedEventMessage::InningEnd { number, side } => {
                    if *number != self.state.inning_number {
                        warn!(
                            "Unexpected inning number in {}: expected {}, but saw {number}",
                            self.game_id,
                            self.state.inning_number,
                        );
                    }

                    if *side != self.state.inning_half {
                        warn!(
                            "Unexpected inning side in {}: expected {:?}, but saw {side:?}",
                            self.game_id,
                            self.state.inning_half,
                        );
                    }

                    // These get cleared at the end of a PA, but the PA doesn't end for an inning-
                    // ending caught stealing
                    self.state.count_balls = 0;
                    self.state.count_strikes = 0;

                    self.state.runners_on.clear();

                    if *number < 9 {
                        // Game never ends if inning number is less than 9
                        self.state.phase = GamePhase::ExpectInningStart;
                    } else if *side == TopBottom::Top && self.state.home_score > self.state.away_score {
                        // Game ends after the top of the inning if it's 9 or later and the home
                        // team is winning
                        self.state.phase = GamePhase::ExpectGameEnd;
                    } else if *side == TopBottom::Bottom && self.state.home_score != self.state.away_score {
                        // Game ends after the bottom of the inning if it's 9 or later and it's not
                        // a tie
                        self.state.phase = GamePhase::ExpectGameEnd;
                    } else {
                        // Otherwise the game does not end
                        self.state.phase = GamePhase::ExpectInningStart;
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
                    self.state.phase = GamePhase::ExpectNowBatting;
                    None
                },
                [ParsedEventMessageDiscriminants::PitcherSwap]
                // TODO handle every single member of this variant
                ParsedEventMessage::PitcherSwap { .. } => {
                    // I think this is not always ExpectNowBatting. I may have
                    // to store the state-to-return-to as a data member of
                    // GamePhase::ExpectMoundVisitOutcome
                    self.state.phase = GamePhase::ExpectNowBatting;
                    None
                },
            ),
            GamePhase::ExpectGameEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::GameOver]
                ParsedEventMessage::GameOver => {
                    self.state.phase = GamePhase::ExpectFinalScore;
                    None
                },
            ),
            GamePhase::ExpectFinalScore => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Recordkeeping]
                // TODO handle every single member of this variant
                ParsedEventMessage::Recordkeeping { winning_score, losing_score, .. } => {
                    if self.state.away_score < self.state.home_score {
                        if *winning_score != self.state.home_score {
                            warn!(
                                "Expected the winning score to be {} (home team) but it was {}",
                                self.state.home_score,
                                winning_score,
                            );
                        }
                        if *losing_score != self.state.away_score {
                            warn!(
                                "Expected the losing score to be {} (away team) but it was {}",
                                self.state.away_score,
                                losing_score,
                            );
                        }
                    } else {
                        if *winning_score != self.state.away_score {
                            warn!(
                                "Expected the winning score to be {} (away team) but it was {}",
                                self.state.away_score,
                                winning_score,
                            );
                        }
                        if *losing_score != self.state.home_score {
                            warn!(
                                "Expected the losing score to be {} (home team) but it was {}",
                                self.state.home_score,
                                losing_score,
                            );
                        }
                    }

                    self.state.phase = GamePhase::Finished;
                    None
                },
            ),
            GamePhase::Finished => game_event!((previous_event, event)),
        }?;

        self.state.prev_event_type = this_event_discriminant;

        let skip_base_check = &[
            // Pending answer from Danny on this one:
            // https://discord.com/channels/1136709081319604324/1370896620199215245
            ("6807e733128045e526322fc6", 75),
            // I think I just need to clear the bases at the last out
            // but I'm not doing it for now
            ("6807e733128045e526322fc3", 33),
            ("6807e733128045e526322fc3", 34),
        ];

        if !skip_base_check.contains(&(self.game_id, index)) {
            self.check_baserunner_consistency(raw_event);
        }

        Ok(result)
    }
}

// This can be disabled once the to-do is addressed
#[allow(unreachable_code, unused_variables)]
fn check_now_batting_stats(stats: &NowBattingStats, batter_stats: &BatterStats) {
    return; // TODO Finish implementing this

    match stats {
        NowBattingStats::FirstPA => {
            if !batter_stats.is_empty() {
                warn!("In NowBatting, expected this batter to have no stats in the current game");
            }
        }
        NowBattingStats::Stats { stats } => {
            let mut their_stats = stats.iter();

            match their_stats.next() {
                None => {
                    warn!("This NowBatting event had stats, but the vec was empty");
                }
                Some(BatterStat::HitsForAtBats { hits, at_bats }) => {
                    if *hits != batter_stats.hits {
                        warn!(
                            "NowBatting said player has {hits} hits, but our records say {}",
                            batter_stats.hits
                        );
                    }
                    if *at_bats != batter_stats.at_bats {
                        warn!(
                            "NowBatting said player has {at_bats} at bats, but our records say {}",
                            batter_stats.at_bats
                        );
                    }
                }
                Some(other) => {
                    warn!("First item in stats was not HitsForAtBats {:?}", other);
                }
            }

            let our_stats = batter_stats.stats.iter();

            for zipped in their_stats.zip_longest(our_stats) {
                match zipped {
                    EitherOrBoth::Left(theirs) => {
                        warn!("NowBatting event had unexpected stat entry {:?}", theirs);
                    }
                    EitherOrBoth::Right(ours) => {
                        warn!("NowBatting missing expected stat entry {:?}", ours);
                    }
                    EitherOrBoth::Both(theirs, ours) => {
                        todo!("Compare {:?} to {:?}", theirs, ours)
                    }
                }
            }
        }
        NowBattingStats::NoStats => {
            todo!("What does this mean?")
        }
    }
}

fn positioned_player_as_ref<StrT: AsRef<str>>(
    p: &EventDetailFielder<StrT>,
) -> PositionedPlayer<&str> {
    PositionedPlayer {
        name: p.name.as_ref(),
        position: p.position.into(),
    }
}

impl<StrT: AsRef<str>> EventDetail<StrT> {
    fn count(&self) -> (u8, u8) {
        (self.count_balls, self.count_strikes)
    }

    fn fielders_iter(&self) -> impl Iterator<Item = PositionedPlayer<&str>> {
        self.fielders.iter().map(positioned_player_as_ref)
    }

    fn fielders(&self) -> Vec<PositionedPlayer<&str>> {
        self.fielders_iter().collect()
    }

    fn steals_iter(&self) -> impl Iterator<Item = BaseSteal<&str>> {
        self.baserunners.iter().flat_map(|runner| {
            if runner.is_steal {
                Some(BaseSteal {
                    runner: runner.name.as_ref(),
                    base: runner.base_after.into(),
                    caught: runner.is_out,
                })
            } else {
                None
            }
        })
    }

    fn steals(&self) -> Vec<BaseSteal<&str>> {
        self.steals_iter().collect()
    }

    // An advance is a baserunner who was on a non-home base before AND after this event
    fn advances_iter(&self) -> impl Iterator<Item = RunnerAdvance<&str>> {
        self.baserunners.iter().flat_map(|runner| {
            let base_before = runner.base_before?;
            let base_after = runner.base_after;

            if runner.is_out || base_after == TaxaBase::Home || base_before == base_after {
                None
            } else {
                Some(RunnerAdvance {
                    runner: runner.name.as_ref(),
                    base: base_after.into(),
                })
            }
        })
    }

    fn advances(&self) -> Vec<RunnerAdvance<&str>> {
        self.advances_iter().collect()
    }

    // A score is any runner whose final base is Home
    fn scores_iter(&self) -> impl Iterator<Item = &str> {
        self.baserunners.iter().flat_map(|runner| {
            if !runner.is_out && runner.base_after == TaxaBase::Home {
                Some(runner.name.as_ref())
            } else {
                None
            }
        })
    }

    fn scores(&self) -> Vec<&str> {
        self.scores_iter().collect()
    }

    // A runner out is any runner where the final base is None
    // Every such runner must have a base_before of Some
    fn runners_out_iter(&self) -> impl Iterator<Item = (&str, BaseNameVariant)> {
        self.baserunners
            .iter()
            .filter(|runner| runner.is_out)
            .map(|runner| {
                let base_format = runner
                    .base_description_format
                    .expect("Runner who got out must have a base_description_format");

                (
                    runner.name.as_ref(),
                    TaxaBaseWithDescriptionFormat(runner.base_after, base_format).into(),
                )
            })
    }

    fn runners_out(&self) -> Vec<(&str, BaseNameVariant)> {
        self.runners_out_iter().collect()
    }

    // TODO Is this debug bound necessary?
    pub fn to_parsed(&self) -> ParsedEventMessage<&str>
    where
        StrT: Debug,
    {
        match self.detail_type {
            TaxaEventType::Ball => ParsedEventMessage::Ball {
                steals: self.steals(),
                count: self.count(),
            },
            TaxaEventType::CalledStrike => {
                let steals = self.steals();
                let caught_steals = steals.iter().filter(|s| s.caught).count();
                if self.outs_after > self.outs_before + (caught_steals as i32) {
                    ParsedEventMessage::StrikeOut {
                        foul: None,
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Looking,
                        steals: self.steals(),
                    }
                } else {
                    ParsedEventMessage::Strike {
                        strike: StrikeType::Looking,
                        steals: self.steals(),
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::SwingingStrike => {
                let steals = self.steals();
                let caught_steals = steals.iter().filter(|s| s.caught).count();
                if self.outs_after > self.outs_before + (caught_steals as i32) {
                    ParsedEventMessage::StrikeOut {
                        foul: None,
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Swinging,
                        steals,
                    }
                } else {
                    ParsedEventMessage::Strike {
                        strike: StrikeType::Swinging,
                        steals,
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::FoulTip => {
                let steals = self.steals();
                let caught_steals = steals.iter().filter(|s| s.caught).count();
                if self.outs_after > self.outs_before + (caught_steals as i32) {
                    ParsedEventMessage::StrikeOut {
                        foul: Some(FoulType::Tip),
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Swinging,
                        steals,
                    }
                } else {
                    ParsedEventMessage::Foul {
                        foul: FoulType::Tip,
                        steals: self.steals(),
                        count: self.count(),
                    }
                }
            },
            TaxaEventType::FoulBall => ParsedEventMessage::Foul {
                foul: FoulType::Ball,
                steals: self.steals(),
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
                    fair_ball_type: self
                        .fair_ball_type
                        .expect("BatterToBase type must have a fair_ball_type")
                        .into(),
                    fielder,
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::ForceOut => {
                let ((runner_out_name, runner_out_at_base),) =
                    self.runners_out_iter().collect_tuple().expect(
                        "ForceOut must have exactly one runner out. TODO Handle this properly.",
                    );

                ParsedEventMessage::ForceOut {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    fair_ball_type: self
                        .fair_ball_type
                        .expect("ForceOut type must have a fair_ball_type")
                        .into(),
                    out: RunnerOut {
                        runner: runner_out_name,
                        base: runner_out_at_base,
                    },
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::CaughtOut => {
                let (caught_by,) = self
                    .fielders_iter()
                    .collect_tuple()
                    .expect("CaughtOut must have exactly one fielder. TODO Handle this properly.");

                let scores = self.scores();
                let fair_ball_type = self
                    .fair_ball_type
                    .expect("CaughtOut type must have a fair_ball_type")
                    .into();
                let is_fly = fair_ball_type != FairBallType::GroundBall;
                let sacrifice = is_fly && !scores.is_empty();

                ParsedEventMessage::CaughtOut {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type,
                    caught_by,
                    scores,
                    advances: self.advances(),
                    sacrifice,
                    perfect: false, // TODO
                }
            }
            TaxaEventType::GroundedOut => ParsedEventMessage::GroundedOut {
                batter: self.batter_name.as_ref(),
                fielders: self.fielders(),
                scores: self.scores(),
                advances: self.advances(),
            },
            TaxaEventType::Walk => ParsedEventMessage::Walk {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(),
            },
            TaxaEventType::HomeRun => {
                let scores = self.scores();
                let grand_slam = scores.len() == 3;
                ParsedEventMessage::HomeRun {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type: self
                        .fair_ball_type
                        .expect("HomeRun type must have a fair_ball_type")
                        .into(),
                    destination: self
                        .fair_ball_direction
                        .expect("HomeRun type must have a fair_ball_direction")
                        .into(),
                    scores,
                    grand_slam,
                }
            }
            TaxaEventType::FieldingError => {
                let (fielder,) = self.fielders_iter().collect_tuple().expect(
                    "FieldingError must have exactly one fielder. TODO Handle this properly.",
                );

                ParsedEventMessage::ReachOnFieldingError {
                    batter: self.batter_name.as_ref(),
                    fielder,
                    error: self
                        .fielding_error_type
                        .expect("FieldingError type must have a fielding_error_type")
                        .into(),
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::HitByPitch => ParsedEventMessage::HitByPitch {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(),
            },
            TaxaEventType::DoublePlay => {
                println!("Baserunners: {:#?}", self.baserunners);
                match &self.runners_out().as_slice() {
                    [] => panic!("At least one existing runner must get out in a DoublePlay"),
                    [(name, at_base)] => {
                        ParsedEventMessage::DoublePlayCaught {
                            batter: self.batter_name.as_ref(),
                            fair_ball_type: self
                                .fair_ball_type
                                .expect("DoublePlay type must have a fair_ball_type")
                                .into(),
                            fielders: self.fielders(),
                            out_two: RunnerOut {
                                runner: name,
                                base: *at_base,
                            },
                            scores: self.scores(),
                            advances: self.advances(),
                        }
                    }
                    [(name_one, base_one), (name_two, base_two)] => {
                        ParsedEventMessage::DoublePlayGrounded {
                            batter: self.batter_name.as_ref(),
                            fielders: self.fielders(),
                            out_one: RunnerOut {
                                runner: name_one,
                                base: *base_one,
                            },
                            out_two: RunnerOut {
                                runner: name_two,
                                base: *base_two,
                            },
                            scores: self.scores(),
                            advances: self.advances(),
                            sacrifice: false, // TODO
                        }
                    }
                    other => {
                        panic!("Too many runners out in double play ({})", other.len());
                    }
                }
            }
            TaxaEventType::FieldersChoice => {
                let ((runner_out_name, runner_out_at_base),) =
                    self.runners_out_iter().collect_tuple().expect(
                        "FieldersChoice must have exactly one runner out. TODO Handle this properly.",
                    );

                ParsedEventMessage::ReachOnFieldersChoice {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    result: FieldingAttempt::Out {
                        out: RunnerOut {
                            runner: runner_out_name,
                            base: runner_out_at_base,
                        }
                    },
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::ErrorOnFieldersChoice => {
                let fielders = self.fielders();
                let fielder = fielders
                    .iter()
                    .exactly_one()
                    .expect(
                        "ErrorOnFieldersChoice must have exactly one fielder (note: this might not be true after supporting FieldingErrorType::Fielding). TODO Handle this properly.",
                    )
                    .name;

                ParsedEventMessage::ReachOnFieldersChoice {
                    batter: self.batter_name.as_ref(),
                    fielders,
                    result: FieldingAttempt::Error {
                        fielder,
                        error: FieldingErrorType::Throwing,
                    },
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
        }
    }

    pub fn to_parsed_contact(&self) -> ParsedEventMessage<&str> {
        // We're going to construct a FairBall for this no matter
        // whether we had the type.
        ParsedEventMessage::FairBall {
            batter: self.batter_name.as_ref(),
            fair_ball_type: self
                .fair_ball_type
                .expect("Event with a fair_ball_index must have a fair_ball_type")
                .into(),
            destination: self
                .fair_ball_direction
                .expect("Event with a fair_ball_index must have a fair_ball_direction")
                .into(),
        }
    }
}
