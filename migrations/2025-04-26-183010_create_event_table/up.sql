create schema data;
create schema taxa;

alter table ingests set schema data;

create table taxa.event_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.hit_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
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
    hit_type bigserial references taxa.hit_type, -- should be populated for every event_type==Hit
    count_balls int not null,
    count_strikes int not null,
    outs_before int not null,
    outs_after int not null,
    ends_inning boolean not null,
    -- note: runs scored, outs on play, steal info, etc. are all computed from data.event_baserunners

    -- player info
    batter_count int not null, -- starts at 0 and increments every time a new batter steps up
    batter_name text not null,
    pitcher_name text not null,
    fielder_names text[] not null
);

create table data.event_baserunners (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigserial references data.events not null,

    -- actual data
    baserunner_name text not null,
    -- base numbering plan:
    --   - 0 is home base, 1 is first, etc.
    --   - the batter is not considered to be "at home". a batter who
    --     reaches first base moves from null to 1
    --   - runners who score always reach base 0 (i.e. their
    --     base_after is 0)
    --   - runners who get out have a base_after of null, including if
    --     by caught stealing
    --   - batters who get out (e.g. strikeout, foul tip, ground out)
    --     do not show up in this table
    --   - i don't know what a row with base_before == null and
    --     base_after == null would mean, but i reserve the right to
    --     have one
    base_before int, -- null == not on base before (i.e. this was the hit/walk/etc that put them on base)
    base_after int, -- null == not on base after because of getting out. if they scored, this will be 0
    steal bool not null -- this records all ATTEMPTED steals. identify failed steals by looking for base_after == null
);