create schema data;
create schema taxa;
create schema info;

-- TODO should this be in info instead?
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

create table taxa.position (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.fair_ball_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.base (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.base_description_format (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.fielding_error_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.pitch_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table data.games (
    -- bookkeeping
    id bigserial primary key not null,
    ingest bigserial references data.ingests not null,
    mmolb_game_id text not null unique, -- note: unique causes an index to be built

    -- game metadata
    season int not null,
    day int not null,
    away_team_emoji text not null,
    away_team_name text not null,
    away_team_id text not null,
    home_team_emoji text not null,
    home_team_name text not null,
    home_team_id text not null
);

create index games_ingest_id_index on data.games (ingest);

create table info.raw_events (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigserial references data.games on delete cascade not null,
    game_event_index int not null,

    -- event data
    event_text text not null
);

create table info.event_ingest_log (
    -- bookkeeping
    id bigserial primary key not null,
    raw_event_id bigserial references info.raw_events on delete cascade not null,

    -- log data
    log_order int not null,
    log_level int not null,
    log_text text not null
);

-- Without this, deleting a game is super slow
create index event_ingest_log_raw_event_id_index on info.event_ingest_log (raw_event_id);

create table info.game_ingest_timing (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigserial references data.games on delete cascade not null,

    check_already_ingested_duration float8 not null,
    network_duration float8 not null,
    parse_duration float8 not null,
    sim_duration float8 not null,
    db_insert_duration float8 not null,
    db_fetch_for_check_duration float8 not null,
    db_fetch_for_check_get_game_id_duration float8 not null,
    db_fetch_for_check_get_events_duration float8 not null,
    db_fetch_for_check_get_runners_duration float8 not null,
    db_fetch_for_check_group_runners_duration float8 not null,
    db_fetch_for_check_get_fielders_duration float8 not null,
    db_fetch_for_check_group_fielders_duration float8 not null,
    db_fetch_for_check_post_process_duration float8 not null,
    db_duration float8 not null,
    check_round_trip_duration float8 not null,
    insert_extra_logs_duration float8 not null,
    total_duration float8 not null
);

create table data.events (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigserial references data.games on delete cascade not null,
    game_event_index int not null,
    -- the event index for the "<player> hit a <distance> to <destination>"
    -- event, if there is one
    fair_ball_event_index int,
    inning int not null,
    top_of_inning boolean not null,

    -- event data
    event_type bigint references taxa.event_type not null,
    -- should be populated for every event_type==Hit
    hit_type bigint references taxa.hit_type,
    -- should be populated for every event type where there's a fair ball
    fair_ball_type bigint references taxa.fair_ball_type,
    fair_ball_direction bigint references taxa.position,
    -- populated when there's a fielding error
    fielding_error_type bigint references taxa.fielding_error_type,
    -- should always be populated for current list of events
    pitch_type bigint references taxa.pitch_type,
    -- should always be populated for current list of events
    pitch_speed float8,
    -- should always be populated for current list of events
    pitch_zone int,
    -- this is specifically *described* as sacrifice, because there is
    -- or was a bug that caused plays to incorrectly be called
    -- sacrifices. The bug is as yet unconfirmed, so it might be
    -- intended behavior, but it still doesn't follow the MLB
    -- definition of a sacrifice.
    -- this is null for events that can never be called sacrifices
    -- (those that can are caught outs and grounded double plays)
    described_as_sacrifice bool,
    count_balls int not null,
    count_strikes int not null,
    outs_before int not null,
    outs_after int not null,
    -- note: runs scored, outs on play, steal info, etc. are all computed from data.event_baserunners

    -- player info
    batter_name text not null,
    pitcher_name text not null
    -- note: more data is in data.event_baserunners and data.event_fielders

    -- fair_ball_event_index, fair_ball_type, and fair_ball_direction should in sync w/r/t null-ness
    -- DEBUG: These constraints are disabled because the nature of a batch insert makes it hard to debug them. I have
    -- Rust-layer checks for violations, and I'll enable these once the Rust ones pass
--     constraint fair_ball_type_null_sync check ((fair_ball_event_index is not null) = (fair_ball_type is not null)),
--     constraint fair_ball_direction_null_sync check ((fair_ball_event_index is not null) = (fair_ball_direction is not null))
 );

create index events_game_id_index on data.events (game_id);

create table data.event_baserunners (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    baserunner_name text not null,
    -- Base numbering plan:
    --   - 0 is home base, 1 is first, etc.
    --   - The batter is not considered to be "at home". A batter who
    --     reaches first base moves from null to 1.
    --   - Runners who score always reach home, meaning they will have
    --     a row in this table where their base_after is 0.
    --   - When a runner is out, they have a row in this table where
    --     is_out is true. They will have a base_after, which indicates
    --     which base they were out at. This is also when
    --     base_description_format becomes relevant, but I don't
    --     recommend assuming base_description_format == null
    --     correlates with is_out == true.
    --   - Batters (who become batter-runners and) who are put out at a
    --     named base have a row in this table where both base_before
    --     is null and is_out is true. Batters who get out in a way
    --     that doesn't name a base don't have a row in this table at
    --     all.
    --   - Runners who don't move during an event have a row in this
    --     table with base_before == base_after.
    --   - Runners stranded on base simply stop showing up in this
    --     table.
    --   - Similarly, the automatic runner who stars on 2nd during
    --     extra innings just starts showing up with a base_before
    --     of 2nd and no previous row with a base_before of null.
    base_before bigint references taxa.base, -- null == not on base before (i.e. this was the hit/walk/etc that put them on base)
    base_after bigint references taxa.base not null, -- `not null` is an experiment, it may have to become nullable
    is_out bool not null,
    base_description_format bigint references taxa.base_description_format, -- null == not applicable because this event didn't name the base in a way that could be formatted
    steal bool not null -- this records all ATTEMPTED steals. identify failed steals by looking at is_out
);

create index event_baserunners_event_id_index on data.event_baserunners (event_id);

create table data.event_fielders (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    fielder_name text not null,
    fielder_position bigint references taxa.position not null,
    play_order int not null,
    perfect_catch bool -- null indicates this was not a catch
);

create index event_fielders_event_id_index on data.event_fielders (event_id);
