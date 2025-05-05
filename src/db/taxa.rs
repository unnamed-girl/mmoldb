use crate::models::{NewEventType, NewHitType, NewPosition, NewFairBallType};
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
// TODO Populate display names with EnumMessage
#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaEventType {
    Ball,
    StrikeLooking,
    StrikeSwinging,
    FoulTip,
    FoulBall,
    Hit,
    ForceOut,
    CaughtOut,
    GroundedOut,
    Walk,
    HomeRun,
    FieldingError,
    HitByPitch,
    DoublePlay,
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
            TaxaPosition::Pitcher => { mmolb_parsing::enums::Position::Pitcher }
            TaxaPosition::Catcher => { mmolb_parsing::enums::Position::Catcher }
            TaxaPosition::FirstBase => { mmolb_parsing::enums::Position::FirstBaseman }
            TaxaPosition::SecondBase => { mmolb_parsing::enums::Position::SecondBaseman }
            TaxaPosition::ThirdBase => { mmolb_parsing::enums::Position::ThirdBaseman }
            TaxaPosition::Shortstop => { mmolb_parsing::enums::Position::ShortStop }
            TaxaPosition::LeftField => { mmolb_parsing::enums::Position::LeftField }
            TaxaPosition::CenterField => { mmolb_parsing::enums::Position::CenterField }
            TaxaPosition::RightField => { mmolb_parsing::enums::Position::RightField }
        }
    }
}

impl From<mmolb_parsing::enums::Position> for TaxaPosition {
    fn from(other: mmolb_parsing::enums::Position) -> Self {
        match other {
            mmolb_parsing::enums::Position::Pitcher => { TaxaPosition::Pitcher }
            mmolb_parsing::enums::Position::Catcher => { TaxaPosition::Catcher }
            mmolb_parsing::enums::Position::FirstBaseman => { TaxaPosition::FirstBase }
            mmolb_parsing::enums::Position::SecondBaseman => { TaxaPosition::SecondBase }
            mmolb_parsing::enums::Position::ThirdBaseman => { TaxaPosition::ThirdBase }
            mmolb_parsing::enums::Position::ShortStop => { TaxaPosition::Shortstop }
            mmolb_parsing::enums::Position::LeftField => { TaxaPosition::LeftField }
            mmolb_parsing::enums::Position::CenterField => { TaxaPosition::CenterField }
            mmolb_parsing::enums::Position::RightField => { TaxaPosition::RightField }
            _ => panic!("TaxaPosition currently only represents defense positions"),
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

impl Into<mmolb_parsing::enums::HitType> for TaxaFairBallType {
    fn into(self) -> mmolb_parsing::enums::HitType {
        match self {
            Self::GroundBall => { mmolb_parsing::enums::HitType::GroundBall }
            Self::FlyBall => { mmolb_parsing::enums::HitType::FlyBall }
            Self::LineDrive => { mmolb_parsing::enums::HitType::LineDrive }
            Self::Popup => { mmolb_parsing::enums::HitType::Popup }
        }
    }
}

impl From<mmolb_parsing::enums::HitType> for TaxaFairBallType {
    fn from(value: mmolb_parsing::enums::HitType) -> Self {
        match value {
            mmolb_parsing::enums::HitType::GroundBall => { Self::GroundBall }
            mmolb_parsing::enums::HitType::FlyBall => { Self::FlyBall }
            mmolb_parsing::enums::HitType::LineDrive => { Self::LineDrive }
            mmolb_parsing::enums::HitType::Popup => { Self::Popup }
        }
    }
}

pub struct Taxa {
    event_type_mapping: EnumMap<TaxaEventType, i64>,
    hit_type_mapping: EnumMap<TaxaHitType, i64>,
    position_mapping: EnumMap<TaxaPosition, i64>,
    fair_ball_type_mapping: EnumMap<TaxaFairBallType, i64>,
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
            event_type_mapping: make_mapping!{
                TaxaEventType,
                crate::taxa_schema::taxa::event_type::dsl::event_type,
                crate::taxa_schema::taxa::event_type::dsl::name,
                crate::taxa_schema::taxa::event_type::dsl::display_name,
                crate::taxa_schema::taxa::event_type::dsl::id,
                conn,
            },
            hit_type_mapping: make_mapping!{
                TaxaHitType,
                crate::taxa_schema::taxa::hit_type::dsl::hit_type,
                crate::taxa_schema::taxa::hit_type::dsl::name,
                crate::taxa_schema::taxa::hit_type::dsl::display_name,
                crate::taxa_schema::taxa::hit_type::dsl::id,
                conn,
            },
            position_mapping: make_mapping!{
                TaxaPosition,
                crate::taxa_schema::taxa::position::dsl::position,
                crate::taxa_schema::taxa::position::dsl::name,
                crate::taxa_schema::taxa::position::dsl::display_name,
                crate::taxa_schema::taxa::position::dsl::id,
                conn,
            },
            fair_ball_type_mapping: make_mapping!{
                TaxaFairBallType,
                crate::taxa_schema::taxa::fair_ball_type::dsl::fair_ball_type,
                crate::taxa_schema::taxa::fair_ball_type::dsl::name,
                crate::taxa_schema::taxa::fair_ball_type::dsl::display_name,
                crate::taxa_schema::taxa::fair_ball_type::dsl::id,
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

    pub fn event_type_from_id(&self, id: i64) -> TaxaEventType {
        self.event_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown event type")
            .0
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
}
