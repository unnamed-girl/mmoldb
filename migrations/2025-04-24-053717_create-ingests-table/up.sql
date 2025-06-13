create table ingests (
    id bigserial primary key not null,
    started_at timestamp without time zone not null,
    -- If this is null the ingest is ongoing or it crashed
    finished_at timestamp without time zone,
    -- If this is null the ingest is ongoing, finished successfully, or
    -- crashed in a way that prevented the database from being updated
    aborted_at timestamp without time zone,

    -- The season number of the latest season that we know we don't
    -- ever need to ingest more games for. If this is null it may mean
    -- that there is no such season, or it may mean this ingest crashed
    -- in a way that prevented the database from being updated. The
    -- proper use of this value is to select the most recent non-null.
    latest_completed_season int
)
