use std::collections::VecDeque;
use std::fmt::Write;
use crate::db::{TaxaEventType, TaxaFairBallType, TaxaPosition, TaxaHitType, TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat};
use itertools::{Itertools, PeekingNext};
use log::{debug, info, warn};
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::{BaseNameVariants, Distance, FieldingErrorType, FoulType, FairBallDestination, FairBallType, HomeAway, StrikeType, TopBottom};
use mmolb_parsing::parsed_event::{ParsedEventMessageDiscriminants, Play, PositionedPlayer, RunnerAdvance, RunnerOut};
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
pub struct EventDetailRunner<StrT> {
    pub name: StrT,
    pub base_before: Option<TaxaBase>,
    pub base_after: Option<TaxaBase>,
    pub base_description_format: Option<TaxaBaseDescriptionFormat>,
    pub is_steal: bool,
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

    pub baserunners: Vec<EventDetailRunner<StrT>>,
}

#[derive(Debug, Copy, Clone)]
struct FairBall {
    index: usize,
    fair_ball_type: FairBallType,
    fair_ball_destination: FairBallDestination
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

#[derive(Debug, Clone)]
struct RunnerOn<'g> {
    runner_name: &'g str,
    base: TaxaBase,
}

#[derive(Debug, Clone)]
struct GameState<'g> {
    prev_event_type: ParsedEventMessageDiscriminants,
    phase: GamePhase,
    inning_number: u8,
    inning_half: TopBottom,
    count_balls: u8,
    count_strikes: u8,
    outs: i32,
    runners_on: VecDeque<RunnerOn<'g>>
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
    } else if !(prev_runner.base <= out.base.into()) {
        // If the base they got out at to is behind the base they started on, no match
        false
    } else {
        true
    }
}

struct EventDetailBuilder<'a, 'g> {
    game: &'a Game<'g>,
    prev_game_state: GameState<'g>,
    game_event_index: usize,
    fair_ball_event_index: Option<usize>,
    fair_ball_type: Option<TaxaFairBallType>,
    fair_ball_direction: Option<TaxaPosition>,
    hit_type: Option<TaxaHitType>,
    fielders: Vec<PositionedPlayer<&'g str>>,
    advances: Vec<RunnerAdvance<&'g str>>,
    scores: Vec<&'g str>,
    runner_added: Option<(&'g str, TaxaBase)>,
    runner_out: Option<RunnerOut<&'g str>>,
}

impl<'a, 'g> EventDetailBuilder<'a, 'g> {
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

        self.advances = advances;
        self.scores = scores;
        self
    }
    
    fn add_runner(mut self, runner_name: &'g str, to_base: TaxaBase) -> Self {
        self.runner_added = Some((runner_name, to_base));
        self
    }
    
    fn add_out(mut self, runner_out: RunnerOut<&'g str>) -> Self {
        self.runner_out = Some(runner_out);
        self
    }

    pub fn build_some(self, type_detail: TaxaEventType) -> Option<EventDetail<&'g str>> {
        Some(self.build(type_detail))
    }

    pub fn build(self, type_detail: TaxaEventType) -> EventDetail<&'g str> {
        let mut runner_state = "Building event with previous runners:".to_string();
        for runner in &self.prev_game_state.runners_on {
            write!(runner_state, "\n    - {} on {}", runner.runner_name, runner.base).unwrap();
        }
        write!(runner_state, "\nand advances").unwrap();
        for advance in &self.advances {
            write!(runner_state, "\n    - {} to {}", advance.runner, advance.base).unwrap();
        }
        info!("{}", runner_state);
        
        let mut scores = self.scores.into_iter();
        let mut advances = self.advances.into_iter().peekable();
        let mut runners_out = self.runner_out.into_iter().peekable();

        let baserunners = self.prev_game_state.runners_on.into_iter()
            .map(|prev_runner| {
                if let Some(scorer_name) = scores.next() {
                    // First: If there are any scores left, they MUST be in runner order. No need
                    // to search for a match.
                    if scorer_name != prev_runner.runner_name {
                        panic!("A runner who was not at the front of the runners list scored!")
                    }
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: Some(TaxaBase::Home),
                        base_description_format: None,
                        is_steal: false,
                    }
                } else if let Some(advance) = advances.peeking_next(|a| is_matching_advance(&prev_runner, a)) {
                    // If the runner didn't score, they may have advanced
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: Some(advance.base.into()),
                        base_description_format: None,
                        is_steal: false,
                    }
                } else if let Some(out) = runners_out.peeking_next(|o| is_matching_runner_out(&prev_runner, o)) {
                    // If the runner didn't score or advance, they may have gotten out
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: None,
                        base_description_format: Some(out.base.into()),
                        is_steal: false,
                    }
                } else {
                    // If the runner didn't score, advance, or get out they just stayed on base
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: Some(prev_runner.base),
                        base_description_format: None,
                        is_steal: false,
                    }
                }
            })
            .chain(
                // Finally, add runners added
                self.runner_added.into_iter()
                    .map(|(name, base)| {
                        EventDetailRunner {
                            name,
                            base_before: None,
                            base_after: Some(base),
                            base_description_format: None,
                            is_steal: false,
                        }
                    })
            )
            .collect();
        
        assert!(scores.next().is_none(), "At least one scoring runner was not found!");
        assert!(advances.next().is_none(), "At least one advancing runner was not found!");
        assert!(runners_out.next().is_none(), "At least one runner out was not found!");

        EventDetail {
            game_id: self.game.game_id,
            game_event_index: self.game_event_index,
            fair_ball_event_index: self.fair_ball_event_index,
            inning: self.game.state.inning_number,
            top_of_inning: self.game.state.inning_half.is_top(),
            count_balls: self.game.state.count_balls,
            count_strikes: self.game.state.count_strikes,
            outs_before: self.prev_game_state.outs,
            outs_after: self.game.state.outs,
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
            baserunners,
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
            state: GameState {
                prev_event_type: ParsedEventMessageDiscriminants::PlayBall,
                phase: GamePhase::ExpectInningStart,
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

    fn defending_team_mut(&mut self) -> &mut TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &mut self.home,
            TopBottom::Bottom => &mut self.away,
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

    fn emit<'a>(&'a self, prev_game_state: GameState<'g>, game_event_index: usize) -> EventDetailBuilder<'a, 'g> {
        EventDetailBuilder {
            game: self,
            prev_game_state,
            fair_ball_event_index: None,
            game_event_index,
            fielders: Vec::new(),
            advances: Vec::new(),
            hit_type: None,
            fair_ball_type: None,
            fair_ball_direction: None,
            scores: Vec::new(),
            runner_added: None,
            runner_out: None,
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
        self.state.count_strikes = 0;
        self.state.count_balls = 0;
        self.state.phase = GamePhase::ExpectNowBatting;
    }

    pub fn add_out(&mut self) {
        debug!("Number of outs at start of event: {}", self.state.outs);
        self.state.outs += 1;

        if self.state.outs >= 3 {
            self.state.phase = GamePhase::ExpectInningEnd;
        } else {
            self.state.phase = GamePhase::ExpectNowBatting;
        }
    }

    pub fn runner_out(&mut self, out: &RunnerOut<&'g str>) {
        self.check_internal_baserunner_consistency();

        // TODO The runner can set out running to a base that had a 
        //   runner on it before and that should be considered a valid
        //   advance candidate. Not sure how to handle this.
        let candidates = self.get_advance_candidates(out.runner, out.base.into());

        if candidates.is_empty() {
            panic!("Tried to put a runner out but there are no matching runners");
        } else {
            if candidates.len() > 1 {
                warn!("Multiple candidates for a runner out. Taking the one who's farthest on the basepaths.");
            }
            self.state.runners_on.remove(candidates[0])
                .expect("Tried to remove a runner who did not exist");
        }
        self.add_out();

        self.check_internal_baserunner_consistency();
    }

    pub fn add_runner(&mut self, runner_name: &'g str, base: TaxaBase) {
        self.state.runners_on.push_back(RunnerOn {
            runner_name,
            base,
        })
    }

    fn check_baserunner_consistency(&self, raw_event: &mmolb_parsing::game::Event) {
        self.check_internal_baserunner_consistency();

        let mut on_1b = false;
        let mut on_2b = false;
        let mut on_3b = false;

        for runner in &self.state.runners_on {
            match runner.base {
                TaxaBase::Home => {}
                TaxaBase::First => { on_1b = true }
                TaxaBase::Second => { on_2b = true }
                TaxaBase::Third => { on_3b = true }
            }
        }

        assert_eq!(on_1b, raw_event.on_1b);
        assert_eq!(on_2b, raw_event.on_2b);
        assert_eq!(on_3b, raw_event.on_3b);
    }

    fn check_internal_baserunner_consistency(&self) {
        assert!(
            self.state.runners_on.iter().is_sorted_by(|a, b| a.base > b.base),
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
        // Scores is easy, runners ALWAYS score in order
        for &scorer in scores {
            match self.state.runners_on.pop_front() {
                None => panic!("Tried to score a runner who didn't exist"),
                Some(s) if s.runner_name == scorer => (),
                Some(s) => panic!("Tried to score {scorer}, but {} was ahead of them on the bases", s.runner_name),
            }
        }
    }

    #[allow(non_snake_case)]
    fn do_advances__dont_call_directly(&mut self, advances: &[RunnerAdvance<&'g str>]) {
        self.check_internal_baserunner_consistency();
        // Be as robust to same-name runners as possible
        // TODO Do I need to iterate advances in reverse?
        for advance in advances {
            let advancing_to_base = advance.base.into();
            let candidates = self.get_advance_candidates(advance.runner, advancing_to_base);

            if candidates.is_empty() {
                panic!("Tried to advance runner but there are no matching runners to advance")
            } else {
                if candidates.len() > 1 {
                    warn!("Multiple candidates for a baserunner advance. Taking the one who's farthest on the basepaths.")
                }
                self.state.runners_on[candidates[0]].base = advancing_to_base;
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
                    },
                    // You can advance if the previous runner is ahead of the base you
                    // are advancing to
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

    fn update_runners(&mut self, scores: &[&'g str], advances: &[RunnerAdvance<&'g str>]) {
        // When these methods say don't call directly, this function is
        // the one they want you to call instead
        self.do_scores__dont_call_directly(scores);
        self.do_advances__dont_call_directly(advances);
    }

    // TODO Every time there's a { .. } in the match arm of an
    //   extract_next!, extract the data. If it's redundant with
    //   something else, check it against that other thing and issue a
    //   warning if it doesn't match. Otherwise, record the data.
    pub fn next(
        &mut self,
        index: usize,
        event: &ParsedEventMessage<&'g str>,
        raw_event: &mmolb_parsing::game::Event,
    ) -> Result<Option<EventDetail<&'g str>>, SimError> {
        let prev_state = self.state.clone();
        let previous_event = self.state.prev_event_type;
        let this_event_discriminant = event.discriminant();

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
               // TODO Handle stats
               ParsedEventMessage::NowBatting { batter: batter_name, stats: _ } => {
                   self.increment_batter_count();
                   self.check_batter(batter_name, event.discriminant());

                   self.state.phase = GamePhase::ExpectPitch;
                   None
               },
               [ParsedEventMessageDiscriminants::MoundVisit]
               // TODO handle every single member of this variant
               ParsedEventMessage::MoundVisit { .. } => {
                   self.state.phase = GamePhase::ExpectMoundVisitOutcome;
                   None
               },
            ),
            GamePhase::ExpectPitch => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Ball]
                // TODO handle every single member of this variant
                ParsedEventMessage::Ball { count, .. } => {
                    self.state.count_balls += 1;
                    self.check_count(*count);

                    self.emit(prev_state, index)
                        .build_some(TaxaEventType::Ball)
                },
                [ParsedEventMessageDiscriminants::Strike]
                // TODO handle every single member of this variant
                ParsedEventMessage::Strike { strike, count, .. } => {
                    self.state.count_strikes += 1;
                    self.check_count(*count);

                    self.emit(prev_state, index)
                        .build_some(match strike {
                            StrikeType::Looking => { TaxaEventType::StrikeLooking }
                            StrikeType::Swinging => { TaxaEventType::StrikeSwinging }
                        })
                },
                [ParsedEventMessageDiscriminants::StrikeOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::StrikeOut { strike, .. } => {
                    if self.state.count_strikes < 2 {
                        warn!("Unexpected strikeout in {}: expected 2 strikes in the count, but there were {}", self.game_id, self.state.count_strikes);
                    }

                    self.finish_pa();
                    self.add_out();

                    self.emit(prev_state, index)
                        .build_some(match strike {
                            StrikeType::Looking => { TaxaEventType::StrikeLooking }
                            StrikeType::Swinging => { TaxaEventType::StrikeSwinging }
                        })
                },
                [ParsedEventMessageDiscriminants::Foul]
                // TODO handle every single member of this variant
                ParsedEventMessage::Foul { foul, count, .. } => {
                    // Falsehoods...
                    if !(*foul == FoulType::Ball && self.state.count_strikes >= 2) {
                        self.state.count_strikes += 1;
                    }
                    self.check_count(*count);

                    self.emit(prev_state, index)
                        .build_some(match foul {
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
                // TODO handle every single member of this variant
                ParsedEventMessage::Walk { batter, advances, scores, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.update_runners(scores, advances);
                    self.add_runner(batter, TaxaBase::First);
                    self.finish_pa();

                    self.emit(prev_state, index)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_runner(batter, TaxaBase::First)
                        .build_some(TaxaEventType::Walk)
                },
                [ParsedEventMessageDiscriminants::HitByPitch]
                // TODO handle every single member of this variant
                ParsedEventMessage::HitByPitch { batter, advances, scores, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.update_runners(scores, advances);
                    self.add_runner(batter, TaxaBase::First);
                    self.finish_pa();

                    self.emit(prev_state, index)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(TaxaEventType::HitByPitch)
                },
            ),
            GamePhase::ExpectFairBallOutcome(fair_ball) => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::CaughtOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::CaughtOut { batter, caught_by, advances, scores, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();
                    self.update_runners(scores, advances);
                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .fielder(*caught_by)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(TaxaEventType::CaughtOut)
                },
                [ParsedEventMessageDiscriminants::GroundedOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::GroundedOut { batter, fielders, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();
                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .fielders(fielders.clone())
                        .build_some(TaxaEventType::GroundedOut)
                },
                [ParsedEventMessageDiscriminants::BatterToBase]
                // TODO handle every single member of this variant
                ParsedEventMessage::BatterToBase { batter, distance, fielder, advances, scores, ..  } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.update_runners(scores, advances);
                    self.add_runner(batter, (*distance).into());

                    info!("BatterToBase with advances: {:?}", advances);

                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .hit_type((*distance).into())
                        .fielder(*fielder)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(TaxaEventType::Hit)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldingError]
                // TODO handle every single member of this variant
                ParsedEventMessage::ReachOnFieldingError { batter, fielder, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_runner(batter, TaxaBase::First);

                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .fielder(*fielder)
                        .build_some(TaxaEventType::FieldingError)
                },
                [ParsedEventMessageDiscriminants::HomeRun]
                // TODO handle every single member of this variant
                ParsedEventMessage::HomeRun { batter, scores, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    // This is the one situation where you can have
                    // scores but no advances, because after everyone
                    // scores there's no one left to advance
                    self.update_runners(scores, &[]);

                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .runner_changes(Vec::new(), scores.clone())
                        .build_some(TaxaEventType::HomeRun)
                },
                [ParsedEventMessageDiscriminants::DoublePlayCaught]
                // TODO handle every single member of this variant
                ParsedEventMessage::DoublePlayCaught { batter, advances, scores, play, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out(); // This is the out for the batter
                    self.update_runners(scores, advances);

                    let Play::Out { out } = play else {
                        panic!("TODO Support when play is an Error");
                    };
                    self.runner_out(out);

                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out)
                        .build_some(TaxaEventType::DoublePlayCaught)
                },
                [ParsedEventMessageDiscriminants::DoublePlayGrounded]
                // TODO handle every single member of this variant
                ParsedEventMessage::DoublePlayGrounded { batter, advances, scores, play_one, fielders, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out(); // This is the out for the batter
                    self.update_runners(scores, advances);

                    let Play::Out { out } = play_one else {
                        panic!("TODO Support when play_one is an Error");
                    };
                    self.runner_out(out);

                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out)
                        .fielders(fielders.clone())
                        .build_some(TaxaEventType::DoublePlayGrounded)
                },
                [ParsedEventMessageDiscriminants::ForceOut]
                // TODO handle every single member of this variant
                ParsedEventMessage::ForceOut { batter, .. } => {
                    self.check_batter(batter, event.discriminant());
                    self.finish_pa();
                    self.add_out();

                    self.emit(prev_state, index)
                        .fair_ball(fair_ball)
                        .build_some(TaxaEventType::ForceOut)
                },
            ),
            GamePhase::ExpectInningEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningEnd]
                ParsedEventMessage::InningEnd { number, side } => {
                    if *number != self.state.inning_number {
                        warn!("Unexpected inning number in {}: expected {}, but saw {number}", self.game_id, self.state.inning_number);
                    }

                    if *side != self.state.inning_half {
                        warn!("Unexpected inning side in {}: expected {:?}, but saw {side:?}", self.game_id, self.state.inning_half);
                    }

                    self.state.runners_on.clear();

                    // TODO Implement the full extra innings rule
                    if *number == 9 {
                        self.state.phase = GamePhase::ExpectGameEnd;
                    } else {
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
                [ParsedEventMessageDiscriminants::PitcherSwap]
                ParsedEventMessage::GameOver => {
                    self.state.phase = GamePhase::ExpectFinalScore;
                    None
                },
            ),
            GamePhase::ExpectFinalScore => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Recordkeeping]
                // TODO handle every single member of this variant
                ParsedEventMessage::Recordkeeping { .. } => {
                    self.state.phase = GamePhase::Finished;
                    None
                },
            ),
            GamePhase::Finished => game_event!((previous_event, event)),
        }?;

        self.state.prev_event_type = this_event_discriminant;

        self.check_baserunner_consistency(raw_event);

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

fn runner_score_as_ref(
    a: &(),
) -> () {
    ()
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
    
    // An advance is a baserunner who was on a non-home base before AND after this event
    fn advances_iter(&self) -> impl Iterator<Item = RunnerAdvance<&str>> {
        self
            .baserunners
            .iter()
            .flat_map(|runner| {
                let base_before = runner.base_before?;
                let base_after = runner.base_after?;
                
                if base_after == TaxaBase::Home || base_before == base_after {
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
        self
            .baserunners
            .iter()
            .flat_map(|runner| {
                if Some(TaxaBase::Home) == runner.base_after {
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
    fn runners_out_iter(&self) -> impl Iterator<Item = (&str, BaseNameVariants)> {
        self
            .baserunners
            .iter()
            .filter(|runner| {
                runner.base_after.is_none()
            })
            .map(|runner| {
                let which_base = runner.base_before
                    .expect("Runner without a base_after must have a base_before");
                let base_format = runner.base_description_format
                    .expect(
                        "In runners_out_iter, runner without a \
                        base_after must have a base_description_format"
                    );
                
                (runner.name.as_ref(), TaxaBaseWithDescriptionFormat(which_base, base_format).into())
            })
    }
    
    fn runners_out(&self) -> Vec<(&str, BaseNameVariants)> {
        self.runners_out_iter().collect()
    }

    pub fn to_parsed(&self) -> ParsedEventMessage<&str> where StrT: Debug {
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
                    fair_ball_type: self.fair_ball_type
                        .expect("BatterToBase type must have a fair_ball_type")
                        .into(),
                    fielder,
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::ForceOut => {
                ParsedEventMessage::ForceOut {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    fair_ball_type: FairBallType::GroundBall, // TODO
                    out: todo!(),
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::CaughtOut => {
                let (caught_by,) = self
                    .fielders_iter()
                    .collect_tuple()
                    .expect("CaughtOut must have exactly one fielder. TODO Handle this properly.");

                ParsedEventMessage::CaughtOut {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type: self
                        .fair_ball_type
                        .expect("CaughtOut type must have a fair_ball_type")
                        .into(),
                    caught_by,
                    scores: self.scores(),
                    advances: self.advances(),
                    sacrifice: false, // TODO
                    perfect: false,   // TODO
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
                ParsedEventMessage::HomeRun {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type: self.fair_ball_type
                        .expect("HomeRun type must have a fair_ball_type")
                        .into(),
                    destination: self.fair_ball_direction
                        .expect("HomeRun type must have a fair_ball_direction")
                        .into(),
                    scores: self.scores(),
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
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::HitByPitch => ParsedEventMessage::HitByPitch {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(),
            },
            TaxaEventType::DoublePlayCaught => {
                println!("Baserunners: {:#?}", self.baserunners);
                let ((runner_out_name, runner_out_previous_base),) = self
                    .runners_out_iter()
                    .collect_tuple()
                    .expect("DoublePlayCaught must have exactly one runner out (not counting the batter out). TODO Handle this properly.");
                
                ParsedEventMessage::DoublePlayCaught {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type: FairBallType::Popup, // TODO
                    fielders: self.fielders(),
                    // TODO Handle Play::Error
                    play: Play::Out {
                        out: RunnerOut {
                            runner: runner_out_name,
                            base: runner_out_previous_base, // TODO This needs an increment
                        },
                    },
                    scores: self.scores(),
                    advances: self.advances(),
                }
            }
            TaxaEventType::DoublePlayGrounded => {
                println!("Baserunners: {:#?}", self.baserunners);
                let ((runner_out_name, runner_out_previous_base),) = self
                    .runners_out_iter()
                    .collect_tuple()
                    .expect("DoublePlayGrounded must have exactly one runner out (not counting the batter out). TODO Handle this properly.");

                ParsedEventMessage::DoublePlayGrounded {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    // TODO Handle Play::Error
                    play_one: Play::Out {
                        out: RunnerOut {
                            runner: runner_out_name,
                            // This almost certainly needs to be stored
                            base: next_base(runner_out_previous_base),
                        },
                    },
                    play_two: Play::Out {
                        out: RunnerOut {
                            runner: self.batter_name.as_ref(),
                            base: BaseNameVariants::FirstBase,
                        },
                    },
                    scores: self.scores(),
                    advances: self.advances(),
                    sacrifice: false, // TODO
                }
            }
        }
    }

    pub fn to_parsed_contact(&self) -> ParsedEventMessage<&str> {
        // We're going to construct a FairBall for this no matter
        // whether we had the type.
        ParsedEventMessage::FairBall {
            batter: self.batter_name.as_ref(),
            fair_ball_type: self.fair_ball_type
                .expect("Event with a fair_ball_index must have a fair_ball_type")
                .into(),
            destination: self.fair_ball_direction
                .expect("Event with a fair_ball_index must have a fair_ball_direction")
                .into(),
        }
    }
}

fn next_base(this_base: BaseNameVariants) -> BaseNameVariants {
    match this_base {
        BaseNameVariants::First => { BaseNameVariants::Second }
        BaseNameVariants::FirstBase => { BaseNameVariants::SecondBase }
        BaseNameVariants::OneB => { BaseNameVariants::TwoB }
        BaseNameVariants::Second => { BaseNameVariants::ThirdBase }
        BaseNameVariants::SecondBase => { BaseNameVariants::Third }
        BaseNameVariants::TwoB => { BaseNameVariants::ThreeB }
        BaseNameVariants::ThirdBase => { BaseNameVariants::Home }
        BaseNameVariants::Third => { BaseNameVariants::Home }
        BaseNameVariants::ThreeB => { BaseNameVariants::Home }
        BaseNameVariants::Home => { BaseNameVariants::Home }
    }
}
