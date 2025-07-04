// TODO: There's a lot of duplicate impls of Into and From in here. Clean them up
use paste::paste;

use super::taxa_macro::*;
use diesel::QueryResult;
use diesel::{PgConnection, RunQueryDsl};
use enum_map::EnumMap;
use rocket_sync_db_pools::diesel::prelude::*;
use std::collections::HashSet;
use log::error;
use crate::parsing_extensions;

taxa! {
    #[
        schema = crate::taxa_schema::taxa::event_type,
        table = crate::taxa_schema::taxa::event_type::dsl::event_type,
        id_column = crate::taxa_schema::taxa::event_type::dsl::id,
    ]
    pub enum TaxaEventType {
        #[display_name: &'a str = "ball"]
        Ball = 0,
        #[display_name: &'a str = "called strike"]
        CalledStrike = 1,
        #[display_name: &'a str = "swinging strike"]
        SwingingStrike = 2,
        #[display_name: &'a str = "foul tip"]
        FoulTip = 3,
        #[display_name: &'a str = "foul ball"]
        FoulBall = 4,
        #[display_name: &'a str = "hit"]
        Hit = 5,
        #[display_name: &'a str = "force out"]
        ForceOut = 6,
        #[display_name: &'a str = "caught out"]
        CaughtOut = 7,
        #[display_name: &'a str = "grounded out"]
        GroundedOut = 8,
        #[display_name: &'a str = "walk"]
        Walk = 9,
        #[display_name: &'a str = "home run"]
        HomeRun = 10,
        #[display_name: &'a str = "fielding error"]
        FieldingError = 11,
        #[display_name: &'a str = "hit by pitch"]
        HitByPitch = 12,
        #[display_name: &'a str = "double play"]
        DoublePlay = 13,
        #[display_name: &'a str = "fielder's choice"]
        FieldersChoice = 14,
        #[display_name: &'a str = "error on fielder's choice"]
        ErrorOnFieldersChoice = 15,
        #[display_name: &'a str = "balk"]
        Balk = 16,
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::hit_type,
        table = crate::taxa_schema::taxa::hit_type::dsl::hit_type,
        id_column = crate::taxa_schema::taxa::hit_type::dsl::id,
    ]
    pub enum TaxaHitType {
        #[base_number: i64 = 1]
        Single = 1,
        #[base_number: i64 = 2]
        Double = 2,
        #[base_number: i64 = 3]
        Triple = 3,
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

taxa! {
    #[
        schema = crate::taxa_schema::taxa::fielder_location,
        table = crate::taxa_schema::taxa::fielder_location::dsl::fielder_location,
        id_column = crate::taxa_schema::taxa::fielder_location::dsl::id,
    ]
    pub enum TaxaFielderLocation {
        // NOTE: IDs here are chosen to match the standard baseball positions
        #[display_name: &'a str = "pitcher", abbreviation: &'a str = "P", area: &'a str = "Infield"]
        Pitcher = 1,
        #[display_name: &'a str = "catcher", abbreviation: &'a str = "C", area: &'a str = "Infield"]
        Catcher = 2,
        #[display_name: &'a str = "first base", abbreviation: &'a str = "1B", area: &'a str = "Infield"]
        FirstBase = 3,
        #[display_name: &'a str = "second base", abbreviation: &'a str = "2B", area: &'a str = "Infield"]
        SecondBase = 4,
        #[display_name: &'a str = "third base", abbreviation: &'a str = "3B", area: &'a str = "Infield"]
        ThirdBase = 5,
        #[display_name: &'a str = "shortstop", abbreviation: &'a str = "SS", area: &'a str = "Infield"]
        Shortstop = 6,
        #[display_name: &'a str = "left field", abbreviation: &'a str = "LF", area: &'a str = "Outfield"]
        LeftField = 7,
        #[display_name: &'a str = "center field", abbreviation: &'a str = "CF", area: &'a str = "Outfield"]
        CenterField = 8,
        #[display_name: &'a str = "right field", abbreviation: &'a str = "RF", area: &'a str = "Outfield"]
        RightField = 9,
    }
}

impl Into<mmolb_parsing::enums::FairBallDestination> for TaxaFielderLocation {
    fn into(self) -> mmolb_parsing::enums::FairBallDestination {
        match self {
            Self::Shortstop => { mmolb_parsing::enums::FairBallDestination::ShortStop }
            Self::Catcher => { mmolb_parsing::enums::FairBallDestination::Catcher }
            Self::Pitcher => { mmolb_parsing::enums::FairBallDestination::Pitcher }
            Self::FirstBase => { mmolb_parsing::enums::FairBallDestination::FirstBase }
            Self::SecondBase => { mmolb_parsing::enums::FairBallDestination::SecondBase }
            Self::ThirdBase => { mmolb_parsing::enums::FairBallDestination::ThirdBase }
            Self::LeftField => { mmolb_parsing::enums::FairBallDestination::LeftField }
            Self::CenterField => { mmolb_parsing::enums::FairBallDestination::CenterField }
            Self::RightField => { mmolb_parsing::enums::FairBallDestination::RightField }
        }
    }
}

impl From<mmolb_parsing::enums::FairBallDestination> for TaxaFielderLocation {
    fn from(value: mmolb_parsing::enums::FairBallDestination) -> Self {
        match value {
            mmolb_parsing::enums::FairBallDestination::ShortStop => { Self::Shortstop }
            mmolb_parsing::enums::FairBallDestination::Catcher => { Self::Catcher }
            mmolb_parsing::enums::FairBallDestination::Pitcher => { Self::Pitcher }
            mmolb_parsing::enums::FairBallDestination::FirstBase => { Self::FirstBase }
            mmolb_parsing::enums::FairBallDestination::SecondBase => { Self::SecondBase }
            mmolb_parsing::enums::FairBallDestination::ThirdBase => { Self::ThirdBase }
            mmolb_parsing::enums::FairBallDestination::LeftField => { Self::LeftField }
            mmolb_parsing::enums::FairBallDestination::CenterField => { Self::CenterField }
            mmolb_parsing::enums::FairBallDestination::RightField => { Self::RightField }
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::slot,
        table = crate::taxa_schema::taxa::slot::dsl::slot,
        id_column = crate::taxa_schema::taxa::slot::dsl::id,
    ]
    pub enum TaxaSlot {
        // IDs here are chosen to match the order on the MMOLB team page
        #[
            display_name: &'a str = "Catcher", 
            abbreviation: &'a str = "C", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(2), // Corresponds to a TaxaFielderLocation id
        ]
        Catcher = 1,
        #[
            display_name: &'a str = "First Base", 
            abbreviation: &'a str = "1B", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(3), // Corresponds to a TaxaFielderLocation id
        ]
        FirstBase = 2,
        #[
            display_name: &'a str = "Second Base", 
            abbreviation: &'a str = "2B", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(4), // Corresponds to a TaxaFielderLocation id
        ]
        SecondBase = 3,
        #[
            display_name: &'a str = "Third Base", 
            abbreviation: &'a str = "3B", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(5), // Corresponds to a TaxaFielderLocation id
        ]
        ThirdBase = 4,
        #[
            display_name: &'a str = "Shortstop", 
            abbreviation: &'a str = "SS", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(6), // Corresponds to a TaxaFielderLocation id
        ]
        Shortstop = 5,
        #[
            display_name: &'a str = "Left Field", 
            abbreviation: &'a str = "LF", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(7), // Corresponds to a TaxaFielderLocation id
        ]
        LeftField = 6,
        #[
            display_name: &'a str = "Center Field", 
            abbreviation: &'a str = "CF", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(8), // Corresponds to a TaxaFielderLocation id
        ]
        CenterField = 7,
        #[
            display_name: &'a str = "Right Field", 
            abbreviation: &'a str = "RF", 
            role: &'a str = "Batter", 
            pitcher_type: Option<&'a str> = None, 
            slot_number: Option<i32> = None, 
            location: Option<i64> = Some(9), // Corresponds to a TaxaFielderLocation id
        ]
        RightField = 8,
        #[
            display_name: &'a str = "Designated Hitter",
            abbreviation: &'a str = "DH",
            role: &'a str = "Batter",
            pitcher_type: Option<&'a str> = None,
            slot_number: Option<i32> = None,
            location: Option<i64> = None, // Corresponds to a TaxaFielderLocation id
        ]
        DesignatedHitter = 9,
        #[
            display_name: &'a str = "Starting Pitcher 1", 
            abbreviation: &'a str = "SP1", 
            role: &'a str = "Pitcher", 
            pitcher_type: Option<&'a str> = Some("Starter"), 
            slot_number: Option<i32> = Some(1), 
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        StartingPitcher1 = 10,
        #[
            display_name: &'a str = "Starting Pitcher 2",
            abbreviation: &'a str = "SP2",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Starter"),
            slot_number: Option<i32> = Some(2),
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        StartingPitcher2 = 11,
        #[
            display_name: &'a str = "Starting Pitcher 3", 
            abbreviation: &'a str = "SP3", 
            role: &'a str = "Pitcher", 
            pitcher_type: Option<&'a str> = Some("Starter"), 
            slot_number: Option<i33> = Some(3), 
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        StartingPitcher3 = 12,
        #[
            display_name: &'a str = "Starting Pitcher 4", 
            abbreviation: &'a str = "SP4", 
            role: &'a str = "Pitcher", 
            pitcher_type: Option<&'a str> = Some("Starter"), 
            slot_number: Option<i44> = Some(4), 
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        StartingPitcher4 = 13,
        #[
            display_name: &'a str = "Starting Pitcher 5", 
            abbreviation: &'a str = "SP5", 
            role: &'a str = "Pitcher", 
            pitcher_type: Option<&'a str> = Some("Starter"), 
            slot_number: Option<i55> = Some(5), 
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        StartingPitcher5 = 14,
        #[
            display_name: &'a str = "Relief Pitcher 1",
            abbreviation: &'a str = "RP1",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Reliever"),
            slot_number: Option<i55> = Some(1),
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        ReliefPitcher1 = 15,
        #[
            display_name: &'a str = "Relief Pitcher 2",
            abbreviation: &'a str = "RP2",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Reliever"),
            slot_number: Option<i55> = Some(2),
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        ReliefPitcher2 = 16,
        #[
            display_name: &'a str = "Relief Pitcher 3",
            abbreviation: &'a str = "RP3",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Reliever"),
            slot_number: Option<i55> = Some(3),
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        ReliefPitcher3 = 17,
        #[
            display_name: &'a str = "Closer",
            abbreviation: &'a str = "CL",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Closer"),
            slot_number: Option<i55> = None,
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        Closer = 18,
        // The following are for past games, where the game event messages
        // didn't announce the specific roster slot. Eventually we hope to
        // remove them, but the work to backfill the data hasn't yet been
        // started.
        // They're also used in case there's a pitcher with too high of a
        // number (e.g. an SP6 or RP4).
        #[
            display_name: &'a str = "Starting Pitcher",
            abbreviation: &'a str = "SP",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Starter"),
            slot_number: Option<i55> = None,
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        StartingPitcher = 19,
        #[
            display_name: &'a str = "Relief Pitcher",
            abbreviation: &'a str = "RP",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Reliever"),
            slot_number: Option<i55> = None,
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        ReliefPitcher = 20,
        #[
            display_name: &'a str = "Pitcher",
            abbreviation: &'a str = "P",
            role: &'a str = "Pitcher",
            pitcher_type: Option<&'a str> = Some("Unknown"),
            slot_number: Option<i55> = None,
            location: Option<i64> = Some(1), // Corresponds to a TaxaFielderLocation id
        ]
        Pitcher = 21,
    }
}

impl From<parsing_extensions::BestEffortSlot> for TaxaSlot {
    fn from(value: parsing_extensions::BestEffortSlot) -> Self {
        match value {
            parsing_extensions::BestEffortSlot::Slot(s) => match s {
                mmolb_parsing::enums::Slot::Catcher => { TaxaSlot::Catcher }
                mmolb_parsing::enums::Slot::FirstBaseman => { TaxaSlot::FirstBase }
                mmolb_parsing::enums::Slot::SecondBaseman => { TaxaSlot::SecondBase }
                mmolb_parsing::enums::Slot::ThirdBaseman => { TaxaSlot::ThirdBase }
                mmolb_parsing::enums::Slot::ShortStop => { TaxaSlot::Shortstop }
                mmolb_parsing::enums::Slot::LeftField => { TaxaSlot::LeftField }
                mmolb_parsing::enums::Slot::CenterField => { TaxaSlot::CenterField }
                mmolb_parsing::enums::Slot::RightField => { TaxaSlot::RightField }
                mmolb_parsing::enums::Slot::StartingPitcher(1) => { TaxaSlot::StartingPitcher1 }
                mmolb_parsing::enums::Slot::StartingPitcher(2) => { TaxaSlot::StartingPitcher2 }
                mmolb_parsing::enums::Slot::StartingPitcher(3) => { TaxaSlot::StartingPitcher3 }
                mmolb_parsing::enums::Slot::StartingPitcher(4) => { TaxaSlot::StartingPitcher4 }
                mmolb_parsing::enums::Slot::StartingPitcher(5) => { TaxaSlot::StartingPitcher5 }
                mmolb_parsing::enums::Slot::StartingPitcher(other) => {
                    error!(
                        "Falling back to non-numbered StartingPitcher for starting pitcher out of \
                        range ({other}).",
                    );
                    TaxaSlot::StartingPitcher
                }
                mmolb_parsing::enums::Slot::ReliefPitcher(1) => { TaxaSlot::ReliefPitcher1 }
                mmolb_parsing::enums::Slot::ReliefPitcher(2) => { TaxaSlot::ReliefPitcher2 }
                mmolb_parsing::enums::Slot::ReliefPitcher(3) => { TaxaSlot::ReliefPitcher3 }
                mmolb_parsing::enums::Slot::ReliefPitcher(other) => {
                    error!(
                        "Falling back to non-numbered ReliefPitcher for relief pitcher out of \
                        range ({other}).",
                    );
                    TaxaSlot::ReliefPitcher
                }
                mmolb_parsing::enums::Slot::Closer => { TaxaSlot::Closer }
                mmolb_parsing::enums::Slot::DesignatedHitter => { TaxaSlot::DesignatedHitter }
            }
            parsing_extensions::BestEffortSlot::SlotType(t) => match t {
                mmolb_parsing::enums::SlotDiscriminants::Catcher => { TaxaSlot::Catcher }
                mmolb_parsing::enums::SlotDiscriminants::FirstBaseman => { TaxaSlot::FirstBase }
                mmolb_parsing::enums::SlotDiscriminants::SecondBaseman => { TaxaSlot::SecondBase }
                mmolb_parsing::enums::SlotDiscriminants::ThirdBaseman => { TaxaSlot::ThirdBase }
                mmolb_parsing::enums::SlotDiscriminants::ShortStop => { TaxaSlot::Shortstop }
                mmolb_parsing::enums::SlotDiscriminants::LeftField => { TaxaSlot::LeftField }
                mmolb_parsing::enums::SlotDiscriminants::CenterField => { TaxaSlot::CenterField }
                mmolb_parsing::enums::SlotDiscriminants::RightField => { TaxaSlot::RightField }
                mmolb_parsing::enums::SlotDiscriminants::StartingPitcher => { TaxaSlot::StartingPitcher }
                mmolb_parsing::enums::SlotDiscriminants::ReliefPitcher => { TaxaSlot::ReliefPitcher }
                mmolb_parsing::enums::SlotDiscriminants::Closer => { TaxaSlot::Closer }
                mmolb_parsing::enums::SlotDiscriminants::DesignatedHitter => { TaxaSlot::DesignatedHitter }
            }
            parsing_extensions::BestEffortSlot::GenericPitcher => { TaxaSlot::Pitcher }
        }
    }
}

impl Into<mmolb_parsing::enums::Place> for TaxaSlot {
    fn into(self) -> mmolb_parsing::enums::Place {
        match self {
            TaxaSlot::Catcher => { mmolb_parsing::enums::Place::Catcher }
            TaxaSlot::FirstBase => { mmolb_parsing::enums::Place::FirstBaseman }
            TaxaSlot::SecondBase => { mmolb_parsing::enums::Place::SecondBaseman }
            TaxaSlot::ThirdBase => { mmolb_parsing::enums::Place::ThirdBaseman }
            TaxaSlot::Shortstop => { mmolb_parsing::enums::Place::ShortStop }
            TaxaSlot::LeftField => { mmolb_parsing::enums::Place::LeftField }
            TaxaSlot::CenterField => { mmolb_parsing::enums::Place::CenterField }
            TaxaSlot::RightField => { mmolb_parsing::enums::Place::RightField }
            TaxaSlot::DesignatedHitter => { mmolb_parsing::enums::Place::DesignatedHitter }
            TaxaSlot::StartingPitcher1 => { mmolb_parsing::enums::Place::StartingPitcher(Some(1)) }
            TaxaSlot::StartingPitcher2 => { mmolb_parsing::enums::Place::StartingPitcher(Some(2)) }
            TaxaSlot::StartingPitcher3 => { mmolb_parsing::enums::Place::StartingPitcher(Some(3)) }
            TaxaSlot::StartingPitcher4 => { mmolb_parsing::enums::Place::StartingPitcher(Some(4)) }
            TaxaSlot::StartingPitcher5 => { mmolb_parsing::enums::Place::StartingPitcher(Some(5)) }
            TaxaSlot::ReliefPitcher1 => { mmolb_parsing::enums::Place::ReliefPitcher(Some(1)) }
            TaxaSlot::ReliefPitcher2 => { mmolb_parsing::enums::Place::ReliefPitcher(Some(2)) }
            TaxaSlot::ReliefPitcher3 => { mmolb_parsing::enums::Place::ReliefPitcher(Some(3)) }
            TaxaSlot::Closer => { mmolb_parsing::enums::Place::Closer }
            TaxaSlot::StartingPitcher => { mmolb_parsing::enums::Place::StartingPitcher(None) }
            TaxaSlot::ReliefPitcher => { mmolb_parsing::enums::Place::ReliefPitcher(None) }
            TaxaSlot::Pitcher => { mmolb_parsing::enums::Place::Pitcher }
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::fair_ball_type,
        table = crate::taxa_schema::taxa::fair_ball_type::dsl::fair_ball_type,
        id_column = crate::taxa_schema::taxa::fair_ball_type::dsl::id,
    ]
    pub enum TaxaFairBallType {
        #[display_name: &'a str = "Ground ball"]
        GroundBall = 1,
        #[display_name: &'a str = "Fly ball"]
        FlyBall = 2,
        #[display_name: &'a str = "Line drive"]
        LineDrive = 3,
        #[display_name: &'a str = "Popup"]
        Popup = 4,
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

taxa! {
    #[
        schema = crate::taxa_schema::taxa::base,
        table = crate::taxa_schema::taxa::base::dsl::base,
        id_column = crate::taxa_schema::taxa::base::dsl::id,
        derive = (PartialOrd,)
    ]
    pub enum TaxaBase {
        #[bases_achieved: i64 = 4]
        Home = 0,
        #[bases_achieved: i64 = 1]
        First = 1,
        #[bases_achieved: i64 = 2]
        Second = 2,
        #[bases_achieved: i64 = 3]
        Third = 3,
    }
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

taxa! {
    #[
        schema = crate::taxa_schema::taxa::base_description_format,
        table = crate::taxa_schema::taxa::base_description_format::dsl::base_description_format,
        id_column = crate::taxa_schema::taxa::base_description_format::dsl::id,
    ]
    pub enum TaxaBaseDescriptionFormat {
        NumberB = 1,  // e.g. "1B"
        Name = 2,     // e.g. "first"
        NameBase = 3, // e.g. "first base"
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

taxa! {
    #[
        schema = crate::taxa_schema::taxa::fielding_error_type,
        table = crate::taxa_schema::taxa::fielding_error_type::dsl::fielding_error_type,
        id_column = crate::taxa_schema::taxa::fielding_error_type::dsl::id,
    ]
    pub enum TaxaFieldingErrorType {
        Fielding = 1,
        Throwing = 2,
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

taxa! {
    #[
        schema = crate::taxa_schema::taxa::pitch_type,
        table = crate::taxa_schema::taxa::pitch_type::dsl::pitch_type,
        id_column = crate::taxa_schema::taxa::pitch_type::dsl::id,
    ]
    pub enum TaxaPitchType {
        #[display_name: &'a str = "Fastball"]
        Fastball = 1,
        #[display_name: &'a str = "Sinker"]
        Sinker = 2,
        #[display_name: &'a str = "Slider"]
        Slider = 3,
        #[display_name: &'a str = "Changeup"]
        Changeup = 4,
        #[display_name: &'a str = "Curveball"]
        Curveball = 5,
        #[display_name: &'a str = "Cutter"]
        Cutter = 6,
        #[display_name: &'a str = "Sweeper"]
        Sweeper = 7,
        #[display_name: &'a str = "Knuckle curve"]
        KnuckleCurve = 8,
        #[display_name: &'a str = "Splitter"]
        Splitter = 9,
    }
}

impl Into<mmolb_parsing::enums::PitchType> for TaxaPitchType {
    fn into(self) -> mmolb_parsing::enums::PitchType {
        match self {
            Self::Changeup => mmolb_parsing::enums::PitchType::Changeup,
            Self::Sinker => mmolb_parsing::enums::PitchType::Sinker,
            Self::Slider => mmolb_parsing::enums::PitchType::Slider,
            Self::Curveball => mmolb_parsing::enums::PitchType::Curveball,
            Self::Cutter => mmolb_parsing::enums::PitchType::Cutter,
            Self::Sweeper => mmolb_parsing::enums::PitchType::Sweeper,
            Self::KnuckleCurve => mmolb_parsing::enums::PitchType::KnuckleCurve,
            Self::Splitter => mmolb_parsing::enums::PitchType::Splitter,
            Self::Fastball => mmolb_parsing::enums::PitchType::Fastball,
        }
    }
}

impl From<mmolb_parsing::enums::PitchType> for TaxaPitchType {
    fn from(value: mmolb_parsing::enums::PitchType) -> Self {
        match value {
            mmolb_parsing::enums::PitchType::Changeup => Self::Changeup,
            mmolb_parsing::enums::PitchType::Sinker => Self::Sinker,
            mmolb_parsing::enums::PitchType::Slider => Self::Slider,
            mmolb_parsing::enums::PitchType::Curveball => Self::Curveball,
            mmolb_parsing::enums::PitchType::Cutter => Self::Cutter,
            mmolb_parsing::enums::PitchType::Sweeper => Self::Sweeper,
            mmolb_parsing::enums::PitchType::KnuckleCurve => Self::KnuckleCurve,
            mmolb_parsing::enums::PitchType::Splitter => Self::Splitter,
            mmolb_parsing::enums::PitchType::Fastball => Self::Fastball,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Taxa {
    event_type_mapping: EnumMap<TaxaEventType, i64>,
    hit_type_mapping: EnumMap<TaxaHitType, i64>,
    fielder_location_mapping: EnumMap<TaxaFielderLocation, i64>,
    slot_mapping: EnumMap<TaxaSlot, i64>,
    fair_ball_type_mapping: EnumMap<TaxaFairBallType, i64>,
    base_mapping: EnumMap<TaxaBase, i64>,
    base_description_format_mapping: EnumMap<TaxaBaseDescriptionFormat, i64>,
    fielding_error_type_mapping: EnumMap<TaxaFieldingErrorType, i64>,
    pitch_type_mapping: EnumMap<TaxaPitchType, i64>,
}

impl Taxa {
    pub fn new(conn: &mut PgConnection) -> QueryResult<Self> {
        Ok(Self {
            event_type_mapping: TaxaEventType::make_id_mapping(conn)?,
            hit_type_mapping: TaxaHitType::make_id_mapping(conn)?,
            // fielder_location_mapping must appear before slot_mapping in the initializer
            // (it doesn't matter what order it is in the struct declaration)
            fielder_location_mapping: TaxaFielderLocation::make_id_mapping(conn)?,
            slot_mapping: TaxaSlot::make_id_mapping(conn)?,
            fair_ball_type_mapping: TaxaFairBallType::make_id_mapping(conn)?,
            base_mapping: TaxaBase::make_id_mapping(conn)?,
            base_description_format_mapping: TaxaBaseDescriptionFormat::make_id_mapping(conn)?,
            fielding_error_type_mapping: TaxaFieldingErrorType::make_id_mapping(conn)?,
            pitch_type_mapping: TaxaPitchType::make_id_mapping(conn)?,
        })
    }

    pub fn event_type_id(&self, ty: TaxaEventType) -> i64 {
        self.event_type_mapping[ty]
    }

    pub fn hit_type_id(&self, ty: TaxaHitType) -> i64 {
        self.hit_type_mapping[ty]
    }

    pub fn fielder_location(&self, ty: TaxaFielderLocation) -> i64 {
        self.fielder_location_mapping[ty]
    }

    pub fn slot_id(&self, ty: TaxaSlot) -> i64 {
        self.slot_mapping[ty]
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

    pub fn pitch_type_id(&self, ty: TaxaPitchType) -> i64 {
        self.pitch_type_mapping[ty]
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

    pub fn fielder_location_from_id(&self, id: i64) -> TaxaFielderLocation {
        self.fielder_location_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown fielder location")
            .0
    }

    pub fn slot_from_id(&self, id: i64) -> TaxaSlot {
        self.slot_mapping
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

    pub fn pitch_type_from_id(&self, id: i64) -> TaxaPitchType {
        self.pitch_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown pitch type")
            .0
    }
}
