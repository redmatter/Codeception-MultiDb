These examples will get you going with MultiDb. It shows the most basic use cases, and some advanced ones.

Please note that the instructions below assumes that you you have run `composer install` at the root level.

## How to setup the database?

Here I assume that you are familiar with MySQL and you have the credentials for `root` database user.

First of all, you need to change the hostname(s) of the database server in [config file](tests/acceptance.suite.yml). In the description below, for the sake of completeness, hostname that is mentioned in the example config file, will be used.

Now create the databases and create `demo` user.

    mysql -u root -h primary.db.example.com -p < tests/_data/setup/primary.sql
    mysql -u root -h secondary.db.example.com -p < tests/_data/setup/secondary.sql

## How to run the example?

Now you need to `build` and `run` test suites.

    ../vendor/bin/codecept build
    ../vendor/bin/codecept run
