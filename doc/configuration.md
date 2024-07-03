## Configuration

As of Ziggurat version 3.13.0, all the official Kafka configs Kafka configurations for [Streams API](https://docs.confluent.io/platform/current/installation/configuration/streams-configs.html), [Consumer API](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html) and [Producer API](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html) are supported.

All Ziggurat configs should be in your `clonfig` `config.edn` under the `:ziggurat` key.

## Table of Contents

1. [General Configurations](#general-configurations)
2. [Stream Router Configurations](#stream-router-configurations)
    - [Channels](#channels)
    - [Producer](#producer)
3. [Batch Routes](#batch-routes)
4. [SSL](#ssl)
5. [StatsD](#statsd)
6. [Sentry](#sentry)
7. [RabbitMQ Connection](#rabbitmq-connection)
8. [RabbitMQ](#rabbitmq)
9. [Retry](#retry)
10. [Jobs](#jobs)
11. [HTTP Server](#http-server)
12. [New Relic](#new-relic)
13. 

```clojure
{:ziggurat  {:app-name            "application_name"
            :nrepl-server         {:port [7011 :int]}
            :stream-router        {:stream-id            {:application-id                 "kafka_consumer_id"
                                                          :bootstrap-servers              "kafka-broker-1:6667,Kafka-broker-2:6667"
                                                          :stream-threads-count           [1 :int]
                                                          :origin-topic                   "kafka-topic-*"
                                                          :oldest-processed-message-in-s  [604800 :int]
                                                          :changelog-topic-replication-factor [3 :int]
                                                          :stream-thread-exception-response [:shutdown-client :keyword]
                                                          ;;channels help you increase the number of parallel processors more than the number of partitions of your topic.
                                                          ;; please see the channels section for more information.
                                                          :channels                           {:channel-1 {:worker-count [10 :int]
                                                                                                         :retry        {:type    [:linear :keyword]
                                                                                                                        :count   [5 :int]
                                                                                                                        :enabled [true :bool]}}}
                                                          :producer   {:bootstrap-servers                     "localhost:9092"
                                                                       :acks                                  "all"
                                                                       :retries-config                        5
                                                                       :max-in-flight-requests-per-connection 5
                                                                       :enable-idempotence                    false
                                                                       :value-serializer                      "org.apache.kafka.common.serialization.StringSerializer"
                                                                       :key-serializer                        "org.apache.kafka.common.serialization.StringSerializer"}}} 
            :batch-routes         {:restaurants-updates-to-non-personalized-es
                                    {:consumer-group-id          "restaurants-updates-consumer"
                                     :bootstrap-servers          "g-gojek-id-mainstream.golabs.io:6668"
                                     :origin-topic               "restaurant-updates-stream"}}
            :ssl                  {:enabled true
                                   :ssl-keystore-location "/location/to/keystore"
                                   :ssl-keystore-password "some-password"
                                   {:jaas {:username "username"
                                           :password "password"
                                           :mechanism "SCRAM_SHA-512"}}}
            :default-api-timeout-ms-config [600000 :int]
            :statsd               {:host    "localhost"
                                   :port    [8125 :int]
                                   :enabled [false :bool]}
            :statsd               {:host    "localhost"
                                   :port    [8125 :int]
                                   :enabled [false :bool]}
            :sentry               {:enabled                   [false :bool]
                                   :dsn                       "dummy"
                                   :worker-count              [5 :int]
                                   :queue-size                [5 :int]
                                   :thread-termination-wait-s [1 :int]}
            :rabbit-mq-connection {:host            "localhost"
                                   :port            [5672 :int]
                                   :prefetch-count  [3 :int]
                                   :username        "guest"
                                   :password        "guest"
                                   :channel-timeout [2000 :int]}
            :rabbit-mq            {:delay       {:queue-name           "application_name_delay_queue"
                                                 :exchange-name        "application_name_delay_exchange"
                                                 :dead-letter-exchange "application_name_instant_exchange"
                                                 :queue-timeout-ms     [5000 :int]}
                                   :instant     {:queue-name    "application_name_instant_queue"
                                                 :exchange-name "application_name_instant_exchange"}
                                   :dead-letter {:queue-name    "application_name_dead_letter_queue"
                                                 :exchange-name "application_name_dead_letter_exchange"}}
            ;; if retry is enabled, messages are retried in RMQ.  If retry is disabled, and :retry is returned from mapper function, messages will be lost.
             :retry                {:count   [5 :int]
                                   :enabled [false :bool]}
            :jobs                 {:instant {:worker-count   [4 :int]
                                             :prefetch-count [4 :int]}}
            :http-server          {:port         [8010 :int]
                                   :graceful-shutdown-timeout-ms [30000 :int]
            :new-relic            {:report-errors [false :bool]}}
            :prometheus           {:port 8002
                                   :enabled [true :bool]}}}
```


## General Configurations

| Configuration                      | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **app-name**                       | `String`   | Yes       | Refers to the name of the application. Used to namespace queues and metrics.                |
| **nrepl-server**                   | `Integer`  | Yes        | Port on which the REPL server will be hosted.                                               |
| **default-api-timeout-ms-config**  | `Integer`  | No        | Specifies the timeout (in milliseconds) for client APIs. Recommended value is 600000 ms.    |

## Stream Router Configurations

| Configuration                      | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **stream-router**                  | `Object`   | Yes       | Configs related to all the Kafka streams the application is reading from.                   |

### Stream Router Properties

| Property                           | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **stream-id**                      | `String`   | Yes       | The identifier of a stream mentioned in `main.clj`. Each stream can read from different Kafka brokers and have different threads. |
| **application-id**                 | `String`   | Yes       | The Kafka consumer group id. [Documentation](https://kafka.apache.org/intro#intro_consumers) |
| **bootstrap-servers**              | `String`   | Yes       | The Kafka brokers that the application will read from. Accepts a comma-separated value.     |
| **stream-threads-count**           | `Integer`  | Yes        | Number of parallel threads to read messages from Kafka. Can scale up to the number of partitions. |
| **stream-thread-exception-response** | `String`   | No        | Action triggered on an uncaught exception. Possible values: `:shutdown-client` (default), `:shutdown-application`, `:replace-thread`. [More info](https://kafka-tutorials.confluent.io/error-handling/kstreams.html?_ga=2.107379330.1454767099.1620795696-1044723812.1563788148) |
| **origin-topic**                   | `String`   | Yes        | The topic that the stream should read from. Can be a regex. Messages from different streams will be passed to the same mapper-function. |
| **oldest-processed-messages-in-s** | `Integer`  | No        | Oldest message processed by the stream in seconds. Default value is 604800 (1 week).        |
| **changelog-topic-replication-factor** | `Integer`  | No        | Internal changelog topic replication factor. Default value is 3.                            |

## Channels

| Property                           | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **worker-count**                   | `Integer`  | Yes       | Number of messages to process in parallel per channel.                                      |
| **retry**                          | `Object`   | No        | Defines channel retries.                                                                    |

#### Retry Properties

| Property                           | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **type**                           | `String`   | Yes       | Type of retry (linear, exponential).                                                        |
| **count**                          | `Integer`  | Yes       | Number of retries before message is sent to channel DLQ.                                    |
| **enabled**                        | `Boolean`  | Yes       | If channel retries are enabled or not.                                                      |

## Producer

| Property                           | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **bootstrap.servers**              | `String`   | Yes       | List of host/port pairs to use for establishing the initial connection to the Kafka cluster.|
| **acks**                           | `String`   | Yes       | Number of acknowledgments the producer requires before considering a request complete. Valid values: [all, -1, 0, 1]. |
| **retries**                        | `Integer`  | No        | Number of retries for any record whose send fails with a potentially transient error.       |
| **key.serializer**                 | `String`   | Yes       | Serializer class for key implementing the `org.apache.kafka.common.serialization.Serializer` interface. |
| **value.serializer**               | `String`   | Yes       | Serializer class for value implementing the `org.apache.kafka.common.serialization.Serializer` interface. |
| **max.in.flight.requests.per.connection** | `Integer`  | No        | Maximum number of unacknowledged requests the client will send on a single connection before blocking. |
| **enable.idempotence**             | `Boolean`  | No        | Ensures that exactly one copy of each message is written in the stream if set to `true`.    |

## Batch Routes

| Key                                 | Data Type | Mandatory | Description                                     |
|-------------------------------------|-----------|-----------|-------------------------------------------------|
| `:batch-routes`                     | Object    | Yes       | Configures batch routes for Kafka consumer API. |
| `:restaurants-updates-to-non-personalized-es` | Object    | Yes       | Batch route name, customize as per application  |
| `:consumer-group-id`                | String    | Yes       | Consumer group ID for the batch route.          |
| `:bootstrap-servers`                | String    | Yes       | Kafka bootstrap servers for the batch route.    |
| `:origin-topic`                     | String    | Yes       | Origin topic for the batch route.               |

## SSL

| Key                                 | Data Type | Mandatory | Description                                                 |
|-------------------------------------|-----------|-----------|-------------------------------------------------------------|
| `:ssl`                              | Object    | Yes       | SSL configuration for Kafka.                                |
| `:enabled`                          | Boolean   | Yes       | Flag to enable SSL.                                         |
| `:ssl-keystore-location`            | String    | Yes       | Location of the SSL keystore.                               |
| `:ssl-keystore-password`            | String    | Yes       | Password for the SSL keystore.                              |
| `:jaas`                             | Object    | Yes       | JAAS configuration for SASL.                                |
| `:username`                         | String    | Yes       | Username for SASL authentication.                           |
| `:password`                         | String    | Yes       | Password for SASL authentication.                           |
| `:mechanism`                        | String    | Yes       | SASL mechanism (e.g., SCRAM-SHA-512).                       |

## StatsD

| Key            | Data Type | Mandatory | Description                     |
|----------------|-----------|-----------|---------------------------------|
| `:statsd`      | Object    | Yes       | Configuration for StatsD.       |
| `:host`        | String    | Yes       | Host for StatsD.                |
| `:port`        | Integer   | Yes       | Port for StatsD.                |
| `:enabled`     | Boolean   | Yes       | Flag to enable StatsD.          |

## Sentry

| Key                                  | Data Type | Mandatory | Description                                       |
|--------------------------------------|-----------|-----------|---------------------------------------------------|
| `:sentry`                            | Object    | Yes       | Configuration for Sentry.                         |
| `:enabled`                           | Boolean   | Yes       | Flag to enable Sentry.                            |
| `:dsn`                               | String    | Yes       | Data Source Name for Sentry.                      |
| `:worker-count`                      | Integer   | Yes       | Number of Sentry workers.                         |
| `:queue-size`                        | Integer   | Yes       | Size of the Sentry queue.                         |
| `:thread-termination-wait-s`         | Integer   | Yes       | Wait time for thread termination in seconds.      |

## RabbitMQ Connection

| Key                                  | Data Type | Mandatory | Description                                    |
|--------------------------------------|-----------|-----------|------------------------------------------------|
| `:rabbit-mq-connection`              | Object    | Yes       | RabbitMQ connection configuration.             |
| `:host`                              | String    | Yes       | Host for RabbitMQ.                             |
| `:port`                              | Integer   | Yes       | Port for RabbitMQ.                             |
| `:prefetch-count`                    | Integer   | No        | Number of messages to prefetch.                |
| `:username`                          | String    | Yes       | Username for RabbitMQ.                         |
| `:password`                          | String    | Yes       | Password for RabbitMQ.                         |
| `:channel-timeout`                   | Integer   | NO        | Channel timeout in milliseconds.  <br/>Default 2000 |

## RabbitMQ

| Key                                  | Data Type | Mandatory | Description                                       |
|--------------------------------------|-----------|-----------|---------------------------------------------------|
| `:rabbit-mq`                         | Object    | Yes       | Configuration for RabbitMQ queues.                |
| `:delay`                             | Object    | Yes       | Delay queue configuration.                        |
| `:queue-name`                        | String    | Yes       | Name of the delay queue.                          |
| `:exchange-name`                     | String    | Yes       | Name of the delay exchange.                       |
| `:dead-letter-exchange`              | String    | Yes       | Dead letter exchange for the delay queue.         |
| `:queue-timeout-ms`                  | Integer   | Yes       | Queue timeout in milliseconds.                    |
| `:instant`                           | Object    | Yes       | Instant queue configuration.                      |
| `:queue-name`                        | String    | Yes       | Name of the instant queue.                        |
| `:exchange-name`                     | String    | Yes       | Name of the instant exchange.                     |
| `:dead-letter`                       | Object    | Yes       | Dead letter queue configuration.                  |
| `:queue-name`                        | String    | Yes       | Name of the dead letter queue.                    |
| `:exchange-name`                     | String    | Yes       | Name of the dead letter exchange.                 |

## Retry

| Configuration                      | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **retry**                          | `Object`   | Yes       | Number of times the message should be retried and if retry flow should be enabled. If retry is disabled, and `:retry` is returned from mapper function, messages will be lost. |

## Jobs

| Configuration                      | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **jobs**                           | `Object`   | Yes       | Number of consumers that should be reading from the retry queues and the prefetch count of each consumer. |

## HTTP Server

| Configuration                      | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **http-server**                    | `Object`   | Yes       | Defines the port and number of threads for the HTTP server. Also controls the graceful shutdown timeout. Default is `30000ms`. |

## New Relic

| Configuration                      | Data Type  | Mandatory | Description                                                                                 |
|------------------------------------|------------|-----------|---------------------------------------------------------------------------------------------|
| **new-relic**                      | `Object`   | No        | If `report-errors` is true, reports an error to New Relic whenever a `:failure` keyword is returned from the mapper-function or an exception is raised. Can be disabled. |


## Prometheus

| Configuration  | Data Type | Mandatory | Description                                                                                                                      |
|----------------|-----------|-----------|----------------------------------------------------------------------------------------------------------------------------------|
| **prometheus** | `Object`  | No        | Prometheus configuration. By default set to ON. Set the port that prometheus server runs on and enabled flag.                    |
| **enabled**    | `bool`    | yes       | Prometheus configuration. By default set to ON. Enables the startup of prometheus server (statsD is not used if this is enabled) |
| **port**       | `int`     | yes       | Prometheus configuration. Default 8002. Specifies the port that prometheus server runs on.                                       |

