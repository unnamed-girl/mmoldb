use std::fmt::{Display, Formatter};
use std::str::FromStr;
use mmolb_parsing::enums::{Place, Position, Slot, SlotDiscriminants};
use mmolb_parsing::parsed_event::PlacedPlayer;
use strum::IntoDiscriminant;

#[derive(Debug, Copy, Clone)]
pub enum BestEffortSlot {
    Slot(Slot),
    SlotType(SlotDiscriminants),
}

impl Display for BestEffortSlot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BestEffortSlot::Slot(s) => write!(f, "{}", s),
            // This might not be right...
            BestEffortSlot::SlotType(st) => write!(f, "{}", st),
        }
    }
}

impl TryFrom<Place> for BestEffortSlot {
    type Error = ();

    fn try_from(value: Place) -> Result<Self, Self::Error> {
        Ok(match value {
            Place::Position(Position::Pitcher) => { return Err(()) }
            Place::Position(Position::Catcher) => { BestEffortSlot::SlotType(SlotDiscriminants::Catcher) }
            Place::Position(Position::FirstBaseman) => { BestEffortSlot::SlotType(SlotDiscriminants::FirstBaseman) }
            Place::Position(Position::SecondBaseman) => { BestEffortSlot::SlotType(SlotDiscriminants::SecondBaseman) }
            Place::Position(Position::ThirdBaseman) => { BestEffortSlot::SlotType(SlotDiscriminants::ThirdBaseman) }
            Place::Position(Position::ShortStop) => { BestEffortSlot::SlotType(SlotDiscriminants::ShortStop) }
            Place::Position(Position::LeftField) => { BestEffortSlot::SlotType(SlotDiscriminants::LeftField) }
            Place::Position(Position::CenterField) => { BestEffortSlot::SlotType(SlotDiscriminants::CenterField) }
            Place::Position(Position::RightField) => { BestEffortSlot::SlotType(SlotDiscriminants::RightField) }
            Place::Position(Position::StartingPitcher) => { BestEffortSlot::SlotType(SlotDiscriminants::StartingPitcher) }
            Place::Position(Position::ReliefPitcher) => { BestEffortSlot::SlotType(SlotDiscriminants::ReliefPitcher) }
            Place::Position(Position::Closer) => { BestEffortSlot::SlotType(SlotDiscriminants::Closer) }
            Place::Slot(slot) => { BestEffortSlot::Slot(slot) }
        })
    }
}

impl FromStr for BestEffortSlot {
    type Err = <Slot as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let slot = s.parse::<Slot>()?;
        Ok(Self::Slot(slot))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BestEffortSlottedPlayer<StrT> {
    pub name: StrT,
    pub slot: BestEffortSlot,
}

impl<StrT> TryFrom<PlacedPlayer<StrT>> for BestEffortSlottedPlayer<StrT> {
    type Error = <Place as TryInto<BestEffortSlot>>::Error;

    fn try_from(value: PlacedPlayer<StrT>) -> Result<Self, Self::Error> {
        Ok(BestEffortSlottedPlayer {
            name: value.name,
            slot: value.place.try_into()?,
        })
    }
}

impl<StrT: Display> Display for BestEffortSlottedPlayer<StrT> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.slot, self.name)
    }
}

pub fn parse_slot_to_discriminant(input: &str) -> Result<SlotDiscriminants, String> {
    Ok(input.parse::<Slot>().map_err(|()| input.to_string())?.discriminant())
}
