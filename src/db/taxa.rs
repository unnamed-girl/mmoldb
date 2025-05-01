use std::collections::HashSet;
use diesel::{QueryResult, ExpressionMethods};
use enum_map::EnumMap;
use rocket_db_pools::diesel::{AsyncPgConnection, RunQueryDsl};
use strum::EnumMessage;
use crate::models::NewEventType;

// strum's Display is used as the code-friendly name and EnumMessage as
// the human-friendly name (with fallback to Display)
#[derive(Debug, enum_map::Enum, Eq, PartialEq, Hash, Copy, Clone, strum::Display, strum::EnumMessage)]
pub enum TaxaEventType {
    Ball,
    StrikeLooking,
    StrikeSwinging,
    FoulTip,
    FoulBall,
    Hit,
    Out,
}

pub struct Taxa {
    mapping: EnumMap<TaxaEventType, i64>,
}

impl Taxa {
    pub async fn new(conn: &mut AsyncPgConnection) -> QueryResult<Self> {
        use crate::taxa_schema::taxa::event_type::dsl::*;

        let mut mapping: EnumMap<TaxaEventType, i64> = EnumMap::default();

        for (taxa, key) in mapping.iter_mut() {
            let code_friendly_name = format!("{}", taxa);
            let human_friendly_name = taxa.get_message()
                .unwrap_or_else(|| &code_friendly_name);
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
            mapping.iter().map(|(key, _)| key).collect::<HashSet<_>>().len(),
            mapping.iter().map(|(_, value)| value).collect::<HashSet<_>>().len(),
        );

        Ok(Self {
            mapping,
        })
    }

    pub fn event_type(&self, ty: TaxaEventType) -> i64 {
        self.mapping[ty]
    }
}