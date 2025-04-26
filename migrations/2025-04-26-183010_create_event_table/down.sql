drop table data.events;
drop table taxa.event_type;
alter table data.ingests set schema public;
drop schema taxa;
drop schema data;
