# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## Unreleased Changes

## 3.1.0-alpha.1 - 2019-10-14
- Adds tracing support to the framework.

## 3.0.0 - 2019-10-04
- Updates kafka streams - 1.1.1 -> 2.1.0
- Changes metrics format
  - Instead of having service name and topic in the metric name, everything is now added to tags
- Middleware
  - Handler-fn will now receive the message as a byte array
  - Channel-fns will now receive the message as a byte array
  - We have provided middlewares, that can be used to deserialize the messages
  - Deadset-get api will now get serialized messages
- Java functions
  - Java functions are now exposed for all public functions in namespaces
- Dependency simplification
  - Removes dependency overrides.


## 3.0.0-alpha.7 - 2019-10-04
- Removes dependency overrides and conflicts
- Adds pedantic warn to generic profile
- Adds pedantic abort to uberjar profile

## 3.0.0-alpha.6 - 2019-08-22
- Fix increment/decrement count to accept both number and map

## 3.0.0-alpha.5 - 2019-08-20
- Exposes Java methods for init, config, producer, sentry, metrics, fixtures namespaces
- Adds util methods for converting data types between java and Clojure

## 3.0.0-alpha.4 - 2019-07-29
- Remove old metrics from being sent

## 3.0.0-alpha.3 - 2019-07-10
- Adds middleware support
- `Breaking Change!` Mapper-function will now receive deserialised message if middleware is not applied
- Deadset view will now return serialized messages

## 3.0.0-alpha.2 - 2019-07-03
- Fixes a bug that incorrectly checked for additional-tags in ziggurat.metrics/merge-tags

## 3.0.0-alpha.1 - 2019-06-28
- Fixes a bug where calling inc-or-dec count without passing additional tags raised and exception

## 3.0.0-alpha - 2019-06-21
- Upgrades kafka streams to version 2.1. Please refer [this](UpgradeGuide.md) to upgrade

## 2.12.4 - 2019-08-22
- Fix increment/decrement count to accept both number and map

## 2.12.3 - 2019-07-26
- Fix functions to either take vector or string as input

## 2.12.2 - 2019-07-03
- Fixes a bug that incorrectly checked for additional-tags in ziggurat.metrics/merge-tags

## 2.12.1 - 2019-06-28
- Fixes a bug where calling inc-or-dec count without passing additional tags raised and exception

## 2.12.0 - 2019-06-17
- Add support for providing a topic-name label in the metrics
- Multiple Kafka producers support in ziggurat (#55)
- Validate stream routes only when modes is not present or it contains stream-server (#59)

## 2.11.1 - 2019-06-04
- Actor stop fn should stop before the Ziggurat state (#53)

## 2.11.0 - 2019-05-31
- Running ziggurat in different modes (#46)

## 2.10.2 - 2019-05-03
- Adds config to change the changelog topic replication factor

## 2.10.1 - 2019-05-02
- dont close the channel on shutdown listener. it is already closed when connection is broken. this prevents topology recovery
- catch message production exception in rabbitmq publisher
- Adds nippy as dependency instead of carmine

## 2.10.0 - 2019-04-11
- Adds macro for setting thread-local context params for logs

## 2.9.2 - 2019-03-31
- Adds deployent stage on CI pipeline
- Initialize reporters before running actor start fn

## 2.9.3-SNAPSHOT - 2019-02-26
- Initialize reporters before running actor start fn

## 2.9.2-SNAPSHOT - 2019-02-26
- Adds deployent stage on CI pipeline

## 2.9.1 - 2019-02-22
- Updates changelog for older releases
- Releases using java 8

## 2.9.0 - 2019-02-21
- **This release has been compiled using java 10. It will not work with
  older versions of java.**
- Adds oldest-processed-message-in-s config
- Adds capabiltiy to filter message based on timestamp
- Fixes bug in deadset API for channel enabled
- Changes namespace of `transformer` into `timestamp-transformer`

## 2.8.1 - 2019-02-18
- Handle Deadset API when retry is disabled
- Fixing message being retried n + 1 times
- Fixing kafka delay calculation

## 2.8.0 - 2019-02-04
- Upgrades kafka streams to 1.1.1
- Adds stream integration tests
- Adds API to flush messages from dead-letter-queue in RabbitMQ

## 2.7.2 - 2018-12-19
- Starts sentry-reporter in on application initialization

## 2.7.1 - 2018-12-19
- removes executor dependency as it was not being used
- updates readme and contribution guidelines
- refactors config files to remove gojek specific configs
- Removes sentry dependency and instead uses sentry-clj.async

## 2.7.0 - 2018-12-12
- Merges lambda commons and adds default configs for missing application specified configs
- Users using `lambda-commons.metrics` should now start using `ziggurat.metrics` to send custom metrics.

## 2.6.3 - 2018-11-30
- Fixes bug where connection to rabbitmq fails when stream routes is not passed in mount/args

## 2.6.2 - 2018-11-29
- Adds support for multipart params over actor routes, moves lein-kibit and eastwood to dev plugins

## 2.6.1 - 2018-11-26
- Changed the order of starting up of ziggurat and actor. First config will be initialized, then actor function will start up and ziggurat start function will start up.
- Apps with (mount/start) in their `start-fn` will no longer work correctly. Users should start using `mount/only` instead.

## 2.6.0 - 2018-11-23
- Removes Yggdrasil, bulwark and ESB log entities dependency
- Removes the `make-config` function from `ziggurat.config` namespace. Users should now use `config-from-env` function instead.

## 2.5.9 - 2018-11-16
- Overrides and exposes kafka streams config: buffered.records.per.partitions and commit.inteval.ms

## 2.5.8 - 2018-11-15
- Fixes bug where rabbitmq connection is established even when retry is disabled and channels are absent in consumer/start-subscribers

## 2.5.7 - 2018-11-14
- Add configuration to read data from earliest offset in kafka

## 2.5.6 - 2018-10-10
- Fixes rabbitmq queue creation when retries are disabled and channels are present

## 2.5.5 - 2018-10-09
- Fixes rabbitmq intialization when retry is disabled but channels are present

## 2.5.4 - 2018-10-09
- Fixes dead set management api to validate the channel names

## 2.5.3 - 2018-10-09
- Starts up rabbitmq connection when channels are present or retry is enabled

## 2.5.2 - 2018-10-09
- Fixes bug around reporting execution time for handler fn

## 2.5.1 - 2018-10-05
- Fix deadset view bug

## 2.5.0 - 2018-10-04
- Adds arbitrary channels for long running jobs
- Fix parallelism for retry workers

## 2.4.0 - 2018-08-08
- Starts sending expiration per message instead of setting it on the queue

## 2.3.0 - 2018-07-16
- Starts calculating timestamp from kafka metadata
- removes deprecated config variables in kafka-streams

## 2.2.1 - 2018-07-10
- Upgraded lambda commons library to 0.3.1

## 2.2.0 - 2018-07-09
- Upgraded lambda commons library

## 2.1.0 - 2018-07-02
- Adds metrics to skipped and retried message
- Retry message when actor raises an expection

## 2.0.0 - 2018-06-15
- Add support for multi stream routes

## 1.3.4 - 2018-06-11
- Fixes replay of messages in dead letter queue.

## 1.3.3 - 2018-06-11
- Bumps up lambda-common version to 0.2.2

## 1.3.2 - 2018-07-07
- Fixes converting message from kafka to clojure hash

## 1.3.1 - 2018-07-07
- Fixes converting message from kafka to clojure hash
- Instruments time of execution of mapper function
- Increments the esb-log-entites version to fetch from 3.18.7 and above

## 1.3.0 - 2018-05-28
- Fixes the consumer to retry the mapper-fn

## 1.2.3 - 2018-05-28
- Uses WallclockTimestampExtractor as timestamp extractor for the streams

## 1.2.2 - 2018-05-24
- Empty release

## 1.2.1 - 2018-05-11
- Always fetches the esb-log-entites version greater than or equal 3.17.11

## 1.2.0 - 2018-05-10
- Bumps up the esb log entities version to 3.17.11
- Fetches config from yggdrasil and if not found fallbacks to env
```bash
Configs added
{
:ziggurat {:yggdrasil {:host "http://localhost"
                       :port [8080 :int]
                       :connection-timeout-in-ms [1000 :int}}
}
```

## 1.1.1 - 2018-05-03
- Bumps up the esb log entities version

## 1.1.0 - 2018-05-03
- Adds ability to pass actor specific routes

## 1.0.9 - 2018-05-02
- Changes dependency from esb-log-client to esb-log-entities

## 1.0.8 - 2018-04-27
- Adds metrics to count throughput
- Changes the job name getting pushed to NR

## 1.0.7 - 2018-04-25
- Adds an `v1/dead_set` to view the dead set messages

## 1.0.6 - 2018-04-25
- Bump version of `com.gojek/sentry`

## 1.0.5 - 2018-04-24
- Fixed a bug in application shutdown: the actor's start-fn was being called instead of the stop-fn.
- Made some functions private.
- Added some docstrings.
- Added Gotchas section to the README.

## 1.0.4 - 2018-04-20
### Added
- Added ziggurat.sentry/report-error to be used by actors.

## 1.0.3 - 2018-04-20
### Changed
- Upgrade esb-log-client version to latest (1.103.0).

## 1.0.2 - 2018-04-20
### Changed
- Various internal refactorings: removed dead code, fixed some spelling mistakes, made some functions private.

## 1.0.1 - 2018-04-18
### Added
- Flag to enable retries and conditionally start the rabbitmq states depending on this flag.

## 1.0.0 - 2018-04-17
### Changed
- Namespace framework configs under `:ziggurat`

## 0.1.0 - 2018-04-17
- Initial release
