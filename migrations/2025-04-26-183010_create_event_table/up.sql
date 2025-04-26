create schema data;
create schema taxa;

alter table ingests set schema data;

create table taxa.event_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null
);

create table data.events (
    -- bookkeeping
    id bigserial primary key not null,
    ingest bigserial references data.ingests not null,
    game_id text not null,
    game_event_index int not null,
    inning int not null,
    top_of_inning boolean not null,

    -- game data
    event_type bigserial references taxa.event_type not null,
    count_balls int not null,
    count_strikes int not null,
    outs_before int not null,
    outs_after int not null,
    ends_inning boolean not null,

    -- player info
    batter_count int not null, -- starts at 0 and increments every time a new batter steps up
    batter_name text not null,
    pitcher_name text not null,
    fielder_names text[] not null
)
