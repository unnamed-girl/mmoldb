// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

use diesel::{ExpressionMethods, QueryDsl, QueryResult};
use rocket_db_pools::diesel::{AsyncPgConnection, RunQueryDsl};
use crate::models::Ingest;

pub async fn latest_ingests(conn: &mut AsyncPgConnection) -> QueryResult<Vec<Ingest>> {
    use crate::schema::ingests::dsl::*;
    ingests
        .limit(10)
        .order(date_started.asc())
        .load::<Ingest>(conn)
        .await
}
