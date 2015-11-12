# Codeception MultiDb Extension

This extension supports multiple dabatase backends, providing equivalant service as the [Db module](http://codeception.com/docs/modules/Db). Additionally it provices capability to safely switch between database connectors and multi-level transaction support.

It is still in development, but is stable enough for anyone to give it a try.

# Installation

Codeception v2.0

```
composer require "natterbox/codeception-multidb: ~1.0.0"
```

A version that supports codeception v2.1 is still under development. You can install it by using the `composer` command below.

```
composer require "natterbox/codeception-multidb: 2.0.x-dev"
```

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

