use rocket_db_pools::diesel::prelude::*;
use chrono::{DateTime, Utc};

// #[derive(Insertable)]
// #[diesel(table_name = ingests)]
// struct Ingest {
//     date_created: DateTime<Utc>,
// }