create table ingests (
    id bigserial primary key not null,
    date_started timestamp without time zone not null,
    -- if this is null it indicates that the ingest is ongoing OR that it crashed
    date_finished timestamp without time zone
)
