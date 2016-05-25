# Codeception MultiDb Extension

This extension supports multiple dabatase backends, providing equivalant service as the [Db module](http://codeception.com/docs/modules/Db). Additionally it provides capability to safely switch between database connectors and multi-level transaction support.

Currently the only database supported is MySQL; but it is pretty straight forward to add support for other databases.

It is still in development, but is stable enough for anyone to give it a try.

# Installation

For codeception v2.0, please use `v1.0.0` using the `composer` command below.

```
composer require "redmatter/codeception-multidb: ~1.0.0"
```

A version that supports codeception v2.1 is under active development. You can install it by using the `composer` command below.

```
composer require "redmatter/codeception-multidb: ~2.0@dev"
```

NOTE: Even though there has not yet been a stable release for `2.0`, it is fully compatible with `1.0` for features and its API. Please do log an issue for any bugs or possible improvemnts you identify.

# Usage

Please see [`DemoCest`](examples/tests/acceptance/DemoCest.php) and [`acceptance.suite.yml`](examples/tests/acceptance.suite.yml) in examples.

In order to avoid MultiDb (or even the Db module from codeception) leaving behind modifications in the database, due to the user aborting a test run by pressing `Ctrl+C`, leading to breaking tests during further runs, consider using the `natterbox/codeception-ctrlc` module.

# How to contribute?

Please feel free to fork and submit a pull request. Bug fixes and general usability comments would be much appreciated.

If you are to contribute a feature or bug-fix, please do log an issue before starting to work on the code. Then branch from the appropriate release branch, to make those changes (see below for details).

Coding standard that is followed here is `PSR-2`.

## Why version `2.0`?

Version `2.0` will be the branch for migrating MultiDb to Codeception version `2.1`.

## What is happening to version `1.0`?

Version `1.0` is still a supported version; any major feature additions or bug fixes will be done for both versions.

## Is `master` the same as `2.0`?

It is essentially the same; but for the purpose of submitting pull requests, you should choose the appropriate branch, depending on the codeception version you have developed against.

