create table ingests (
    id bigserial primary key not null,
    started_at timestamp without time zone not null,
    -- If this is null the ingest is ongoing or it crashed
    finished_at timestamp without time zone,
    -- If this is null the ingest is ongoing, finished successfully, or
    -- crashed in a way that prevented the database from being updated
    aborted_at timestamp without time zone
)
