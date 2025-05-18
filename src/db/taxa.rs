use crate::models::{
    NewBase, NewBaseDescriptionFormat, NewEventType, NewFairBallType, NewFieldingErrorType,
    NewHitType, NewPosition,
};
use diesel::{ExpressionMethods, QueryResult};
use enum_map::EnumMap;
use rocket_db_pools::diesel::{AsyncPgConnection, RunQueryDsl};
use std::collections::HashSet;
use strum::EnumMessage;

trait AsInsertable<'a> {
    type Insertable;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable;
}

// strum's Display is used as the code-friendly name and EnumMessage as
// the human-friendly name (with fallback to Display)
#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaEventType {
    #[strum(message = "ball")]
    Ball,
    #[strum(message = "called strike")]
    CalledStrike,
    #[strum(message = "swinging strike")]
    SwingingStrike,
    #[strum(message = "foul tip")]
    FoulTip,
    #[strum(message = "foul ball")]
    FoulBall,
    #[strum(message = "hit")]
    Hit,
    #[strum(message = "force out")]
    ForceOut,
    #[strum(message = "caught out")]
    CaughtOut,
    #[strum(message = "grounded out")]
    GroundedOut,
    #[strum(message = "walk")]
    Walk,
    #[strum(message = "home run")]
    HomeRun,
    #[strum(message = "fielding error")]
    FieldingError,
    #[strum(message = "hit by pitch")]
    HitByPitch,
    #[strum(message = "double play")]
    DoublePlay,
    #[strum(message = "fielder's choice")]
    FieldersChoice,
    #[strum(message = "error on fielder's choice")]
    ErrorOnFieldersChoice,
}

impl<'a> AsInsertable<'a> for TaxaEventType {
    type Insertable = NewEventType<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewEventType {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaHitType {
    Single,
    Double,
    Triple,
}

impl<'a> AsInsertable<'a> for TaxaHitType {
    type Insertable = NewHitType<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewHitType {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

impl Into<mmolb_parsing::enums::Distance> for TaxaHitType {
    fn into(self) -> mmolb_parsing::enums::Distance {
        match self {
            Self::Single => mmolb_parsing::enums::Distance::Single,
            Self::Double => mmolb_parsing::enums::Distance::Double,
            Self::Triple => mmolb_parsing::enums::Distance::Triple,
        }
    }
}

impl From<mmolb_parsing::enums::Distance> for TaxaHitType {
    fn from(other: mmolb_parsing::enums::Distance) -> Self {
        match other {
            mmolb_parsing::enums::Distance::Single => Self::Single,
            mmolb_parsing::enums::Distance::Double => Self::Double,
            mmolb_parsing::enums::Distance::Triple => Self::Triple,
        }
    }
}

#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaPosition {
    Pitcher,
    Catcher,
    FirstBase,
    SecondBase,
    ThirdBase,
    Shortstop,
    LeftField,
    CenterField,
    RightField,
    StartingPitcher,
    ReliefPitcher,
    Closer,
    DesignatedHitter,
}

impl<'a> AsInsertable<'a> for TaxaPosition {
    type Insertable = NewPosition<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewPosition {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

impl Into<mmolb_parsing::enums::Position> for TaxaPosition {
    fn into(self) -> mmolb_parsing::enums::Position {
        match self {
            TaxaPosition::Pitcher => mmolb_parsing::enums::Position::Pitcher,
            TaxaPosition::Catcher => mmolb_parsing::enums::Position::Catcher,
            TaxaPosition::FirstBase => mmolb_parsing::enums::Position::FirstBaseman,
            TaxaPosition::SecondBase => mmolb_parsing::enums::Position::SecondBaseman,
            TaxaPosition::ThirdBase => mmolb_parsing::enums::Position::ThirdBaseman,
            TaxaPosition::Shortstop => mmolb_parsing::enums::Position::ShortStop,
            TaxaPosition::LeftField => mmolb_parsing::enums::Position::LeftField,
            TaxaPosition::CenterField => mmolb_parsing::enums::Position::CenterField,
            TaxaPosition::RightField => mmolb_parsing::enums::Position::RightField,
            TaxaPosition::StartingPitcher => mmolb_parsing::enums::Position::StartingPitcher,
            TaxaPosition::ReliefPitcher => mmolb_parsing::enums::Position::ReliefPitcher,
            TaxaPosition::Closer => mmolb_parsing::enums::Position::Closer,
            TaxaPosition::DesignatedHitter => mmolb_parsing::enums::Position::DesignatedHitter,
        }
    }
}

impl From<mmolb_parsing::enums::Position> for TaxaPosition {
    fn from(other: mmolb_parsing::enums::Position) -> Self {
        match other {
            mmolb_parsing::enums::Position::Pitcher => TaxaPosition::Pitcher,
            mmolb_parsing::enums::Position::Catcher => TaxaPosition::Catcher,
            mmolb_parsing::enums::Position::FirstBaseman => TaxaPosition::FirstBase,
            mmolb_parsing::enums::Position::SecondBaseman => TaxaPosition::SecondBase,
            mmolb_parsing::enums::Position::ThirdBaseman => TaxaPosition::ThirdBase,
            mmolb_parsing::enums::Position::ShortStop => TaxaPosition::Shortstop,
            mmolb_parsing::enums::Position::LeftField => TaxaPosition::LeftField,
            mmolb_parsing::enums::Position::CenterField => TaxaPosition::CenterField,
            mmolb_parsing::enums::Position::RightField => TaxaPosition::RightField,
            mmolb_parsing::enums::Position::StartingPitcher => TaxaPosition::StartingPitcher,
            mmolb_parsing::enums::Position::ReliefPitcher => TaxaPosition::ReliefPitcher,
            mmolb_parsing::enums::Position::Closer => TaxaPosition::Closer,
            mmolb_parsing::enums::Position::DesignatedHitter => TaxaPosition::DesignatedHitter,
        }
    }
}

impl TryInto<mmolb_parsing::enums::FairBallDestination> for TaxaPosition {
    type Error = Self;

    fn try_into(self) -> Result<mmolb_parsing::enums::FairBallDestination, Self::Error> {
        match self {
            TaxaPosition::Pitcher => Ok(mmolb_parsing::enums::FairBallDestination::Pitcher),
            TaxaPosition::Catcher => Ok(mmolb_parsing::enums::FairBallDestination::Catcher),
            TaxaPosition::FirstBase => Ok(mmolb_parsing::enums::FairBallDestination::FirstBase),
            TaxaPosition::SecondBase => Ok(mmolb_parsing::enums::FairBallDestination::SecondBase),
            TaxaPosition::ThirdBase => Ok(mmolb_parsing::enums::FairBallDestination::ThirdBase),
            TaxaPosition::Shortstop => Ok(mmolb_parsing::enums::FairBallDestination::ShortStop),
            TaxaPosition::LeftField => Ok(mmolb_parsing::enums::FairBallDestination::LeftField),
            TaxaPosition::CenterField => Ok(mmolb_parsing::enums::FairBallDestination::CenterField),
            TaxaPosition::RightField => Ok(mmolb_parsing::enums::FairBallDestination::RightField),
            TaxaPosition::StartingPitcher => Err(TaxaPosition::StartingPitcher),
            TaxaPosition::ReliefPitcher => Err(TaxaPosition::ReliefPitcher),
            TaxaPosition::Closer => Err(TaxaPosition::Closer),
            TaxaPosition::DesignatedHitter => Err(TaxaPosition::DesignatedHitter),
        }
    }
}

impl From<mmolb_parsing::enums::FairBallDestination> for TaxaPosition {
    fn from(other: mmolb_parsing::enums::FairBallDestination) -> Self {
        match other {
            mmolb_parsing::enums::FairBallDestination::Pitcher => TaxaPosition::Pitcher,
            mmolb_parsing::enums::FairBallDestination::Catcher => TaxaPosition::Catcher,
            mmolb_parsing::enums::FairBallDestination::FirstBase => TaxaPosition::FirstBase,
            mmolb_parsing::enums::FairBallDestination::SecondBase => TaxaPosition::SecondBase,
            mmolb_parsing::enums::FairBallDestination::ThirdBase => TaxaPosition::ThirdBase,
            mmolb_parsing::enums::FairBallDestination::ShortStop => TaxaPosition::Shortstop,
            mmolb_parsing::enums::FairBallDestination::LeftField => TaxaPosition::LeftField,
            mmolb_parsing::enums::FairBallDestination::CenterField => TaxaPosition::CenterField,
            mmolb_parsing::enums::FairBallDestination::RightField => TaxaPosition::RightField,
        }
    }
}

#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaFairBallType {
    GroundBall,
    FlyBall,
    LineDrive,
    Popup,
}

impl<'a> AsInsertable<'a> for TaxaFairBallType {
    type Insertable = NewFairBallType<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewFairBallType {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

impl Into<mmolb_parsing::enums::FairBallType> for TaxaFairBallType {
    fn into(self) -> mmolb_parsing::enums::FairBallType {
        match self {
            Self::GroundBall => mmolb_parsing::enums::FairBallType::GroundBall,
            Self::FlyBall => mmolb_parsing::enums::FairBallType::FlyBall,
            Self::LineDrive => mmolb_parsing::enums::FairBallType::LineDrive,
            Self::Popup => mmolb_parsing::enums::FairBallType::Popup,
        }
    }
}

impl From<mmolb_parsing::enums::FairBallType> for TaxaFairBallType {
    fn from(value: mmolb_parsing::enums::FairBallType) -> Self {
        match value {
            mmolb_parsing::enums::FairBallType::GroundBall => Self::GroundBall,
            mmolb_parsing::enums::FairBallType::FlyBall => Self::FlyBall,
            mmolb_parsing::enums::FairBallType::LineDrive => Self::LineDrive,
            mmolb_parsing::enums::FairBallType::Popup => Self::Popup,
        }
    }
}

#[derive(
    Debug,
    enum_map::Enum,
    Eq,
    PartialEq,
    Hash,
    Copy,
    Clone,
    strum::Display,
    strum::EnumMessage,
    Ord,
    PartialOrd,
)]
#[repr(i32)]
pub enum TaxaBase {
    Home = 0,
    First = 1,
    Second = 2,
    Third = 3,
}

impl TaxaBase {
    pub fn next_base(self) -> TaxaBase {
        match self {
            TaxaBase::Home => TaxaBase::First,
            TaxaBase::First => TaxaBase::Second,
            TaxaBase::Second => TaxaBase::Third,
            TaxaBase::Third => TaxaBase::Home,
        }
    }
}

impl<'a> AsInsertable<'a> for TaxaBase {
    type Insertable = NewBase<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewBase {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

impl Into<mmolb_parsing::enums::Base> for TaxaBase {
    fn into(self) -> mmolb_parsing::enums::Base {
        match self {
            Self::Home => mmolb_parsing::enums::Base::Home,
            Self::First => mmolb_parsing::enums::Base::First,
            Self::Second => mmolb_parsing::enums::Base::Second,
            Self::Third => mmolb_parsing::enums::Base::Third,
        }
    }
}

impl From<mmolb_parsing::enums::Base> for TaxaBase {
    fn from(value: mmolb_parsing::enums::Base) -> Self {
        match value {
            mmolb_parsing::enums::Base::Home => Self::Home,
            mmolb_parsing::enums::Base::First => Self::First,
            mmolb_parsing::enums::Base::Second => Self::Second,
            mmolb_parsing::enums::Base::Third => Self::Third,
        }
    }
}

impl From<mmolb_parsing::enums::Distance> for TaxaBase {
    fn from(value: mmolb_parsing::enums::Distance) -> Self {
        match value {
            mmolb_parsing::enums::Distance::Single => Self::First,
            mmolb_parsing::enums::Distance::Double => Self::Second,
            mmolb_parsing::enums::Distance::Triple => Self::Third,
        }
    }
}

impl From<mmolb_parsing::enums::BaseNameVariant> for TaxaBase {
    fn from(value: mmolb_parsing::enums::BaseNameVariant) -> Self {
        match value {
            mmolb_parsing::enums::BaseNameVariant::First => TaxaBase::First,
            mmolb_parsing::enums::BaseNameVariant::FirstBase => TaxaBase::First,
            mmolb_parsing::enums::BaseNameVariant::OneB => TaxaBase::First,
            mmolb_parsing::enums::BaseNameVariant::Second => TaxaBase::Second,
            mmolb_parsing::enums::BaseNameVariant::SecondBase => TaxaBase::Second,
            mmolb_parsing::enums::BaseNameVariant::TwoB => TaxaBase::Second,
            mmolb_parsing::enums::BaseNameVariant::ThirdBase => TaxaBase::Third,
            mmolb_parsing::enums::BaseNameVariant::Third => TaxaBase::Third,
            mmolb_parsing::enums::BaseNameVariant::ThreeB => TaxaBase::Third,
            mmolb_parsing::enums::BaseNameVariant::Home => TaxaBase::Home,
        }
    }
}

#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaBaseDescriptionFormat {
    NumberB,  // e.g. "1B"
    Name,     // e.g. "first"
    NameBase, // e.g. "first base"
}

impl<'a> AsInsertable<'a> for TaxaBaseDescriptionFormat {
    type Insertable = NewBaseDescriptionFormat<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewBaseDescriptionFormat {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

// Newtype just to hang a From impl on
pub struct TaxaBaseWithDescriptionFormat(pub TaxaBase, pub TaxaBaseDescriptionFormat);

impl Into<mmolb_parsing::enums::BaseNameVariant> for TaxaBaseWithDescriptionFormat {
    fn into(self) -> mmolb_parsing::enums::BaseNameVariant {
        match (self.0, self.1) {
            (TaxaBase::First, TaxaBaseDescriptionFormat::NumberB) => {
                mmolb_parsing::enums::BaseNameVariant::OneB
            }
            (TaxaBase::First, TaxaBaseDescriptionFormat::Name) => {
                mmolb_parsing::enums::BaseNameVariant::First
            }
            (TaxaBase::First, TaxaBaseDescriptionFormat::NameBase) => {
                mmolb_parsing::enums::BaseNameVariant::FirstBase
            }
            (TaxaBase::Second, TaxaBaseDescriptionFormat::NumberB) => {
                mmolb_parsing::enums::BaseNameVariant::TwoB
            }
            (TaxaBase::Second, TaxaBaseDescriptionFormat::Name) => {
                mmolb_parsing::enums::BaseNameVariant::Second
            }
            (TaxaBase::Second, TaxaBaseDescriptionFormat::NameBase) => {
                mmolb_parsing::enums::BaseNameVariant::SecondBase
            }
            (TaxaBase::Third, TaxaBaseDescriptionFormat::NumberB) => {
                mmolb_parsing::enums::BaseNameVariant::ThreeB
            }
            (TaxaBase::Third, TaxaBaseDescriptionFormat::Name) => {
                mmolb_parsing::enums::BaseNameVariant::Third
            }
            (TaxaBase::Third, TaxaBaseDescriptionFormat::NameBase) => {
                mmolb_parsing::enums::BaseNameVariant::ThirdBase
            }
            (TaxaBase::Home, _) => mmolb_parsing::enums::BaseNameVariant::Home,
        }
    }
}

impl From<mmolb_parsing::enums::BaseNameVariant> for TaxaBaseDescriptionFormat {
    fn from(value: mmolb_parsing::enums::BaseNameVariant) -> Self {
        match value {
            mmolb_parsing::enums::BaseNameVariant::First => TaxaBaseDescriptionFormat::Name,
            mmolb_parsing::enums::BaseNameVariant::FirstBase => TaxaBaseDescriptionFormat::NameBase,
            mmolb_parsing::enums::BaseNameVariant::OneB => TaxaBaseDescriptionFormat::NumberB,
            mmolb_parsing::enums::BaseNameVariant::Second => TaxaBaseDescriptionFormat::Name,
            mmolb_parsing::enums::BaseNameVariant::SecondBase => {
                TaxaBaseDescriptionFormat::NameBase
            }
            mmolb_parsing::enums::BaseNameVariant::TwoB => TaxaBaseDescriptionFormat::NumberB,
            mmolb_parsing::enums::BaseNameVariant::ThirdBase => TaxaBaseDescriptionFormat::NameBase,
            mmolb_parsing::enums::BaseNameVariant::Third => TaxaBaseDescriptionFormat::Name,
            mmolb_parsing::enums::BaseNameVariant::ThreeB => TaxaBaseDescriptionFormat::NumberB,
            mmolb_parsing::enums::BaseNameVariant::Home => TaxaBaseDescriptionFormat::Name,
        }
    }
}

#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaFieldingErrorType {
    Fielding,
    Throwing,
}

impl<'a> AsInsertable<'a> for TaxaFieldingErrorType {
    type Insertable = NewFieldingErrorType<'a>;

    fn as_insertable(&self, code_friendly_name: &'a str) -> Self::Insertable {
        let human_friendly_name = self.get_message().unwrap_or_else(|| &code_friendly_name);

        NewFieldingErrorType {
            name: &code_friendly_name,
            display_name: &human_friendly_name,
        }
    }
}

impl Into<mmolb_parsing::enums::FieldingErrorType> for TaxaFieldingErrorType {
    fn into(self) -> mmolb_parsing::enums::FieldingErrorType {
        match self {
            Self::Fielding => mmolb_parsing::enums::FieldingErrorType::Fielding,
            Self::Throwing => mmolb_parsing::enums::FieldingErrorType::Throwing,
        }
    }
}

impl From<mmolb_parsing::enums::FieldingErrorType> for TaxaFieldingErrorType {
    fn from(value: mmolb_parsing::enums::FieldingErrorType) -> Self {
        match value {
            mmolb_parsing::enums::FieldingErrorType::Fielding => Self::Fielding,
            mmolb_parsing::enums::FieldingErrorType::Throwing => Self::Throwing,
        }
    }
}

pub struct Taxa {
    event_type_mapping: EnumMap<TaxaEventType, i64>,
    hit_type_mapping: EnumMap<TaxaHitType, i64>,
    position_mapping: EnumMap<TaxaPosition, i64>,
    fair_ball_type_mapping: EnumMap<TaxaFairBallType, i64>,
    base_mapping: EnumMap<TaxaBase, i64>,
    base_description_format_mapping: EnumMap<TaxaBaseDescriptionFormat, i64>,
    fielding_error_type_mapping: EnumMap<TaxaFieldingErrorType, i64>,
}

macro_rules! make_mapping {
    {
        $taxa_enum:ty,
        $table_name:path,
        $name_field:path,
        $display_name_field:path,
        $id_field:path,
        $conn:expr,
    } => {{
        {
            let mut mapping: EnumMap<$taxa_enum, i64> = EnumMap::default();

            for (taxa, key) in mapping.iter_mut() {
                let code_friendly_name = format!("{}", taxa);
                let new_taxa = taxa.as_insertable(&code_friendly_name);

                *key = diesel::insert_into($table_name)
                    .values(&new_taxa)
                    .on_conflict($name_field)
                    .do_update()
                    .set($display_name_field.eq(&new_taxa.display_name))
                    .returning($id_field)
                    .get_result($conn)
                    .await?;
            }

            // Final safety check: Mapping should hold all distinct values
            // Implemented as # of unique keys == # of unique values
            assert_eq!(
                mapping
                    .iter()
                    .map(|(key, _)| key)
                    .collect::<HashSet<_>>()
                    .len(),
                mapping
                    .iter()
                    .map(|(_, value)| value)
                    .collect::<HashSet<_>>()
                    .len(),
            );

            mapping
        }
    }};
}

impl Taxa {
    pub async fn new(conn: &mut AsyncPgConnection) -> QueryResult<Self> {
        Ok(Self {
            event_type_mapping: make_mapping! {
                TaxaEventType,
                crate::taxa_schema::taxa::event_type::dsl::event_type,
                crate::taxa_schema::taxa::event_type::dsl::name,
                crate::taxa_schema::taxa::event_type::dsl::display_name,
                crate::taxa_schema::taxa::event_type::dsl::id,
                conn,
            },
            hit_type_mapping: make_mapping! {
                TaxaHitType,
                crate::taxa_schema::taxa::hit_type::dsl::hit_type,
                crate::taxa_schema::taxa::hit_type::dsl::name,
                crate::taxa_schema::taxa::hit_type::dsl::display_name,
                crate::taxa_schema::taxa::hit_type::dsl::id,
                conn,
            },
            position_mapping: make_mapping! {
                TaxaPosition,
                crate::taxa_schema::taxa::position::dsl::position,
                crate::taxa_schema::taxa::position::dsl::name,
                crate::taxa_schema::taxa::position::dsl::display_name,
                crate::taxa_schema::taxa::position::dsl::id,
                conn,
            },
            fair_ball_type_mapping: make_mapping! {
                TaxaFairBallType,
                crate::taxa_schema::taxa::fair_ball_type::dsl::fair_ball_type,
                crate::taxa_schema::taxa::fair_ball_type::dsl::name,
                crate::taxa_schema::taxa::fair_ball_type::dsl::display_name,
                crate::taxa_schema::taxa::fair_ball_type::dsl::id,
                conn,
            },
            base_mapping: make_mapping! {
                TaxaBase,
                crate::taxa_schema::taxa::base::dsl::base,
                crate::taxa_schema::taxa::base::dsl::name,
                crate::taxa_schema::taxa::base::dsl::display_name,
                crate::taxa_schema::taxa::base::dsl::id,
                conn,
            },
            base_description_format_mapping: make_mapping! {
                TaxaBaseDescriptionFormat,
                crate::taxa_schema::taxa::base_description_format::dsl::base_description_format,
                crate::taxa_schema::taxa::base_description_format::dsl::name,
                crate::taxa_schema::taxa::base_description_format::dsl::display_name,
                crate::taxa_schema::taxa::base_description_format::dsl::id,
                conn,
            },
            fielding_error_type_mapping: make_mapping! {
                TaxaFieldingErrorType,
                crate::taxa_schema::taxa::fielding_error_type::dsl::fielding_error_type,
                crate::taxa_schema::taxa::fielding_error_type::dsl::name,
                crate::taxa_schema::taxa::fielding_error_type::dsl::display_name,
                crate::taxa_schema::taxa::fielding_error_type::dsl::id,
                conn,
            },
        })
    }

    pub fn event_type_id(&self, ty: TaxaEventType) -> i64 {
        self.event_type_mapping[ty]
    }

    pub fn hit_type_id(&self, ty: TaxaHitType) -> i64 {
        self.hit_type_mapping[ty]
    }

    pub fn position_id(&self, ty: TaxaPosition) -> i64 {
        self.position_mapping[ty]
    }

    pub fn fair_ball_type_id(&self, ty: TaxaFairBallType) -> i64 {
        self.fair_ball_type_mapping[ty]
    }

    pub fn base_id(&self, ty: TaxaBase) -> i64 {
        self.base_mapping[ty]
    }

    pub fn base_description_format_id(&self, ty: TaxaBaseDescriptionFormat) -> i64 {
        self.base_description_format_mapping[ty]
    }

    pub fn fielding_error_type_id(&self, ty: TaxaFieldingErrorType) -> i64 {
        self.fielding_error_type_mapping[ty]
    }

    pub fn event_type_from_id(&self, id: i64) -> Option<TaxaEventType> {
        self.event_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .map(|(val, _)| val)
    }

    pub fn hit_type_from_id(&self, id: i64) -> TaxaHitType {
        self.hit_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown hit type")
            .0
    }

    pub fn position_from_id(&self, id: i64) -> TaxaPosition {
        self.position_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown position")
            .0
    }

    pub fn fair_ball_type_from_id(&self, id: i64) -> TaxaFairBallType {
        self.fair_ball_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown fair ball type")
            .0
    }

    pub fn base_from_id(&self, id: i64) -> TaxaBase {
        self.base_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown base type")
            .0
    }

    pub fn base_description_format_from_id(&self, id: i64) -> TaxaBaseDescriptionFormat {
        self.base_description_format_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown base description format")
            .0
    }

    pub fn fielding_error_type_from_id(&self, id: i64) -> TaxaFieldingErrorType {
        self.fielding_error_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown base description format")
            .0
    }
}
