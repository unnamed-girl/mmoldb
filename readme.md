MMOLDB
======

MMOLDB is a (to-be-)queryable database for [MMOLB][mmolb]. It's currently 
hosted [here][mmoldb].

Running it yourself
-------------------

There are two options to run MMOLDB. One is through devcontainers using the 
file at `.devcontainers/devcontainer.json`. I do my development using 
[RustRover][rustrover]'s devcontainer support. I don't really understand how
devcontainers work myself so I won't give any more information than that.

The other option, which I can provide more support for, is using docker 
compose directly.

Note: It was previously necessary to add `-f docker-compose-prod.yml` to all
docker compose commands. That is no longer necessary, and in fact if you do
that you will get an error. You don't need any `-f` any more. (If for some
reason you really want one, now use `-f docker-prod/docker-compose.yml`.)

First-run Setup
---------------

1. Install [docker-compose][docker-compose]. The installation instructions 
   for docker-compose will also install docker for you.
2. Create the file `.db_admin_password` at the root of this repo, with a secure
   password as the file's contents. 

Running
-------

1. From the root `mmoldb` directory, start the database container: 
   `docker compose up -d db`. This command will run the database container in 
   the background (remove `-d` if you want it to run in the foreground). It 
   will build the container first if necessary.
2. Once the database is up (you can verify that it's up by running 
   `docker compose logs db` and looking for "database system is ready to accept 
   connections"), run the app: `docker compose up -d app`. As before, it will
   be built if necessary and `-d` makes it run in the background.
3. Visit localhost:42424 to see the MMOLDB status page.

Updating
--------

Updating typically requires rebuilding your database, which is currently a 
manual process:

1. `git pull` to fetch the changes
2. `docker compose down` to stop the running containers. Do _not_ add the `-v` 
   flag as was previously recommended -- that will remove the HTTP cache as 
   well as the database itself, and will make your database rebuild 
   significantly slower.
3. `docker compose build` to rebuild the container. If you don't do this you 
   won't see the changes.
4. `docker volume rm mmoldb_postgres-data` to remove the database volume.
5. Run the app again using the "Running" instructions.

Debug
-----

Aside from running in a devcontainer as described earlier, you can get some 
debug information by adding the line `log_level = "normal"` to the `[default]`
section of `Rocket.toml`. Log levels higher than `"normal"` will print debug
information from the libraries mmoldb uses, which is rarely useful. Note that
if you change `Rocket.toml` you will have to rebuild the container before the
change takes effect.

Exiting/Resetting
-----------------

Docker will continue running the db and app containers until you quit them. 
Depending on the docker configuration, it may even relaunch them after a 
reboot. To stop running them, run 
`docker compose -f docker-compose-prod.yml down`.

If you want to stop running the containers *and* delete the database, add the
`-v` flag:
`docker compose -f docker-compose-prod.yml down -v`

mmoldb is not designed to be a primary source for MMOLB data, so it should 
always be safe to delete the database. All that's required to rebuild it is
time and the availability of the actual primary sources.

Contributing
------------

Contributions are very welcome! There are guides on how to contribute specific
things in the `contributing` folder. Contributions that contribute additional
guides are also welcome.

[mmolb]: https://mmolb.com/
[mmoldb]: https://mmoldb.beiju.me/
[rustrover]: https://www.jetbrains.com/rust/
[docker-compose]: https://docs.docker.com/compose/