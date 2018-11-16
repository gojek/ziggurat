# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

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
