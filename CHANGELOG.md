# Change Log

All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## 3.7.1
- Strict type checking for batch handler return type. Application is stopped if the type does not match the expected.   

## 3.7.0
- Added support to flatten protobuf-struct during deserialization

## 3.6.2
- Validation of stream and batch route arguments against the configuration, when starting the application.

## 3.6.1
- Changed the logic for committing offsets to only commit only when non-zero records are polled while 
consuming via Kafka Consumer API

## 3.6.0
- Error reporting done to newrelic along with sentry.

## 3.5.3
- Refactored and simplified the code for retrying, publishing and consuming using RabbitMQ.
- The config `{:messaging {:constructor` has been removed from `:ziggurat` config space
- Both, `{:ziggurat :rabbit-mq-connection {:hosts`  and `{:ziggurat :rabbit-mq-connection {:host` configs 
are accepted for connecting to RabbitMQ. But, `:hosts` is preferred over `:host`. `:hosts` should be used
to define cluster hosts.
- defrecord `ziggurat.mapper.MessagePayload` has been added back to preserve backward compatibility
- Fixed a bug in calculation of exponential backoff timeout where casting the timeout to integer 
was throwing an IllegalArgumentException  

## 3.5.2
- If there's an exception in the batch handler function, the failure metrics is published with a count of "total batch 
size" (which was being processed by the function) instead of just 1 as was being done before this change. 

## 3.5.1
- Fixed publishing of metrics for batch consumption
- Fixed the startup logic for batch consumption - only routes provided in Ziggurat init-args will be started
- Standardized naming for Kafka Consumer API configs

## 3.5.0

- Adds support for consuming Kafka messages in batches using Kafka Consumer API
- Fixes the logic during RabbitMQ disconnection - Ziggurat now retries (publishing a message) 
infinitely till RMQ recovers. This changes the present behaviour where Kafka Streams were being stopped
during disconnection with RabbitMQ.

## 3.4.2-alpha.4

- Log metadata thru sentry

## 3.4.2-alpha.3

- Fix project version

## 3.4.2-alpha.2

- Log the stream record metadata

## 3.4.2-alpha.1

- Investigate if transform() affects how stream joins behave

## 3.4.2

- Fixes issue [#56](https://github.com/gojek/ziggurat/issues/56)
- Adds functionality to stop and restart KafkaStreams using nREPL.

## 3.4.1

- Fixes issue [#142](https://github.com/gojek/ziggurat/issues/142)
- Adds improved error message responses for Deadset API calls

## 3.4.0 - 2020-08-21

- Releases Stable Ziggurat version with support for RabbitMQ clusters

## 3.3.1-alpha.10 - 2020-08-19

- Moves stream joins behind an alpha feature flag

## 3.3.1-alpha.9 - 2020-08-07

- Defaults RabbitMQ queue replication to `(n/2) + 1` nodes, where `n` is the number of nodes in the cluster

## 3.3.1-alpha.8 - 2020-07-27

- Fixes false positive exception thrown by messaging when an actor abnormally terminates

## 3.3.1-alpha.7 - 2020-07-23

- Remove defrecord wrappers

## 3.3.1-alpha.6 - 2020-07-18

- Uses default ha-config values if nothing is provided

## 3.3.1-alpha.5 - 2020-07-06

- Adds RabbitMQMessaging implementation to support connection with RabbitMQ clusters
- Adds support for setting up HA policies for queues and exchanges

## 3.3.1-alpha.4 - 2020-07-02

- Removes the use of the old protobuf library in favor of the new one

## 3.3.1-alpha.3 - 2020-06-29

- Makes messaging implementation configurable
- Adds a new protocol for Messaging

## 3.3.1-alpha.2 - 2020-06-23

- Refactors rabbitmq specific logic to the messaging.rabbitmq package
- Adds unit tests for rabbitmq specific namespaces
- Adds test annotations to messaging integration tests

## 3.3.1-alpha.1 - 2020-06-18

- Support for Kafka Stream KStream-KStream Join

## 3.3.1 - 2020-06-17

- Introduces a swagger middleware on the HTTP server.

## 3.3.0 - 2020-05-26

- Makes metrics library configurable, exposes a metrics interface and provides an
  implementation for clj-statsd library.
- Updates dependency of flatland/protobuf and puts the change behind a alpha-features configuration flag
- Adds support for all configurations of kafka-producer.

_(for a more details list of changes look at the changelogs of 3.3.0-alpha. entries)_

## 3.3.0-alpha.8 - 2020-05-19

- Adding support for all configurations supported by Kafka Producer

## 3.3.0-alpha.7 - 2020-05-07

- Remove `[org.flatland/protobuf "0.8.1"]` from test dependencies

## 3.3.0-alpha.6 - 2020-05-07

- Fixes [Issue](https://github.com/gojek/ziggurat/issues/144)
- Adds alpha feature flags

## 3.3.0-alpha.5 - 2020-04-28

- Makes metrics implementation configurable
- passes "all" metric-name to update timing for all metrics
- Adds docs for MetricsProtocol

## 3.3.0-alpha.4 - 2020-04-26

- Defines an interface for metrics.
- Changes the dropwizard implementation to use the metrics interface
- Adds a metrics interface implementation for clj-statsd library.

## 3.3.0-alpha.3 - 2020-04-22

- Fixes metrics initialization

## 3.3.0-alpha.2 - 2020-04-21

- Refactors Metrics.clj
- Moves dropwizard metrics logic to its own namespace
- Moves statsd state (transport and reporter) to dropwizard namespace

## 3.2.3 - 2020-04-21

- Removes wrap-with-metrics middleware from HTTP router

## 3.2.2 - 2020-04-14

- Fixes bug [Issue](https://github.com/gojek/ziggurat/issues/28)
  to avoid confusion between datadog and statsd

## 3.2.1 - 2020-03-23

- Fixes bug for this [Issue](https://github.com/gojek/ziggurat/issues/131)
- Releases metrics for http requests in a stable release

## 3.3.0-alpha.1 - 2020-01-24

- Added metrics for recording http requests served

## 3.2.0 - 2020-01-09

- Changes Exponential backoff config contract.
  - Adds a `:type` key to retry-config
  - Adds a limit on the number of retries possible in exponential backoff
  - Releasing exponential backoff as an alpha feature
- Fixes [issue](https://github.com/gojek/ziggurat/issues/136) where dead-set replay doesn't send the message to the retry-flow
- Fixes [issue](https://github.com/gojek/ziggurat/issues/129) by updating tools.nrepl dependency to nrepl/nrepl
- Fixes [this bug](https://github.com/gojek/ziggurat/issues/133) where dead set replay broke on Ziggurat upgrade from 2.x to 3.x .
- Fixes [this bug](https://github.com/gojek/ziggurat/issues/115) in RabbitMQ message processing flow
- Adds support for exponential backoffs in channels and normal retry flow
- exponential backoffs can be enabled from the config

## 3.2.0-alpha.5 - 2019-12-17

- Fixes [issue](https://github.com/gojek/ziggurat/issues/136) where dead-set replay doesn't send the message to the retry-flow

## 3.2.0-alpha.4 - 2019-12-17

- Fixes [issue](https://github.com/gojek/ziggurat/issues/129) by updating tools.nrepl dependency to nrepl/nrepl

## 3.2.0-alpha.3 - 2019-12-16

- Fixes [this bug](https://github.com/gojek/ziggurat/issues/133) where dead set replay broke on Ziggurat upgrade from 2.x to 3.x .

## 3.2.0-alpha.2 - 2019-12-12

- Fixes [this bug](https://github.com/gojek/ziggurat/issues/115) in RabbitMQ message processing flow

## 3.2.0-alpha.1 - 2019-12-12

- Adds support for exponential backoffs in channels and normal retry flow
- exponential backoffs can be enabled from the config

## 3.1.0 - 2019-12-6

- Adds tracing support. With [Jaeger](https://www.jaegertracing.io/) as the default tracer
- Adds a JSON middleware to parse JSON serialized functions
- Renames report-time to report-histogram and adds deprecation notice on report-time
- Makes metrics backward compatible with 2.x and 3.0.0 . Ziggurat now publishes metrics in 2 formats similar to version 2.12.0 and above.

## 3.1.0-alpha.5 - 2019-12-5

- Fixes metrics publishing for custom metrics (i.e. string metric namespaces): In 2.x ziggurat appended the service_name
  to a string metrics namespace (e.g. "metric" -> "service_name.metric"), we changed the contract in 3.0 by
  removing the service_name from the metric name and instead adding a tag for it. To have backward compatibility with both
  2.x and 3.0 we now send metrics in both formats

## 3.1.0-alpha.4 - 2019-12-4

- Reintroduces old metrics format (statsd). Ziggurat now pushes metrics in both formats (statsd and prometheus like).
- Reverts the changes for exponential backoff, the current implementation was broken and a new PR is being raised with the correct approach.

## 3.1.0-alpha.3 - 2019-11-11

- Renames report-time to report-histogram while being backward compatible

## 3.1.0-alpha.2 - 2019-11-05

- JSON middleware has been added.
- Adds custom delay ([Issue#78](https://github.com/gojek/ziggurat/issues/78))
  for processing messages from RabbitMQ channels and
  adds exponential backoff strategy (configurable) for channel retries.

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
