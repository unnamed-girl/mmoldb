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
            Place::Pitcher => { return Err(()); }
            Place::Catcher => { BestEffortSlot::Slot(Slot::Catcher) }
            Place::FirstBaseman => { BestEffortSlot::Slot(Slot::FirstBaseman) }
            Place::SecondBaseman => { BestEffortSlot::Slot(Slot::SecondBaseman) }
            Place::ThirdBaseman => { BestEffortSlot::Slot(Slot::ThirdBaseman) }
            Place::ShortStop => { BestEffortSlot::Slot(Slot::ShortStop) }
            Place::LeftField => { BestEffortSlot::Slot(Slot::LeftField) }
            Place::CenterField => { BestEffortSlot::Slot(Slot::CenterField) }
            Place::RightField => { BestEffortSlot::Slot(Slot::RightField) }
            Place::StartingPitcher(Some(i)) => { BestEffortSlot::Slot(Slot::StartingPitcher(i)) }
            Place::ReliefPitcher(Some(i)) => { BestEffortSlot::Slot(Slot::ReliefPitcher(i)) }
            Place::Closer => { BestEffortSlot::Slot(Slot::Closer) }
            Place::DesignatedHitter => { BestEffortSlot::Slot(Slot::DesignatedHitter) }
            Place::StartingPitcher(None) => { BestEffortSlot::SlotType(SlotDiscriminants::StartingPitcher) }
            Place::ReliefPitcher(None) => { BestEffortSlot::SlotType(SlotDiscriminants::ReliefPitcher) }
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
