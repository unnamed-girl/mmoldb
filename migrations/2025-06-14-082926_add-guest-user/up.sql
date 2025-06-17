-- Diesel database reset doesn't delete users, so there are reasonable
-- situations where this up.sql will get run and guest already exists
drop user if exists guest;

create user guest with password 'moldybees';
grant pg_read_all_data to guest;