use crate::models::NewEventType;
use diesel::{ExpressionMethods, QueryResult};
use enum_map::EnumMap;
use rocket_db_pools::diesel::{AsyncPgConnection, RunQueryDsl};
use std::collections::HashSet;
use strum::EnumMessage;

// strum's Display is used as the code-friendly name and EnumMessage as
// the human-friendly name (with fallback to Display)
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
    Out,
    Walk,
    HomeRun,
    FieldingError,
    HitByPitch,
    DoublePlay,
}
#[derive(
    Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage,
)]
pub enum TaxaHitType {
    Single,
    Double,
    Triple,
}

pub struct Taxa {
    event_type_mapping: EnumMap<TaxaEventType, i64>,
    hit_type_mapping: EnumMap<TaxaHitType, i64>,
}

impl Taxa {
    async fn make_mapping_for<EnumT>(
        conn: &mut AsyncPgConnection,
    ) -> QueryResult<EnumMap<EnumT, i64>>
    where
        EnumT: enum_map::EnumArray<i64> + std::fmt::Display + EnumMessage + Eq + std::hash::Hash,
    {
        use crate::taxa_schema::taxa::event_type::dsl::*;

        let mut mapping: EnumMap<EnumT, i64> = EnumMap::default();

        for (taxa, key) in mapping.iter_mut() {
            let code_friendly_name = format!("{}", taxa);
            let human_friendly_name = taxa.get_message().unwrap_or_else(|| &code_friendly_name);
            let n = NewEventType {
                name: &code_friendly_name,
                display_name: &human_friendly_name,
            };

            *key = diesel::insert_into(event_type)
                .values(&n)
                .on_conflict(name)
                .do_update()
                .set(display_name.eq(&human_friendly_name))
                .returning(id)
                .get_result(conn)
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

        Ok(mapping)
    }

    pub async fn new(conn: &mut AsyncPgConnection) -> QueryResult<Self> {
        Ok(Self {
            event_type_mapping: Self::make_mapping_for::<TaxaEventType>(conn).await?,
            hit_type_mapping: Self::make_mapping_for::<TaxaHitType>(conn).await?,
        })
    }

    pub fn event_type(&self, ty: TaxaEventType) -> i64 {
        self.event_type_mapping[ty]
    }

    pub fn hit_type(&self, ty: TaxaHitType) -> i64 {
        self.hit_type_mapping[ty]
    }
}
