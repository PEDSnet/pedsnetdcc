# pedsnetdcc

`pedsnetdcc` is a CLI tool for PEDSnet data coordinating center ETL tasks.

## Usage

### Prepare a database

The `prepdb` subcommand creates a new database for a specific data model version, e.g. `pedsnet_dcc_v23` for `2.3.0`, and schemas within the database.  It presupposes that the database has been initialized and the correct system users have been created using https://github.research.chop.edu/dbhi/pedsnet/tree/master/db/.

    docker build -t pedsnetdcc .
    docker run -it --rm --link pedsnet_postgres_1:db pedsnetdcc prepdb -v 2.3.0 -p postgresql://$USER@db/postgres

Breakdown:

The first line builds the `pedsnetdcc` image.

The second line creates and runs a container based on that image, and throws it away afterwards (`--rm`). The `--link` phrase connects the alias `db` (used later in the command) to the PEDSnet database container (`pedsnet_postgres_1`).  `pedsnetdcc` is the name of the image. Everything from `prepdb` onward is the arguments passed to the `pedsnetdcc` command inside the container.  This command requires that the $USER be a superuser in the database.

### TODO

...

## Random notes

* If you are using `pedsnetdcc` installed via Python instead of using Docker, you might be interested in Click auto-completion: http://click.pocoo.org/5/bashcomplete/.

