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
            :new-relic            {:report-errors [false :bool]}}}}
```


## General Configurations

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **app-name**                       | `String`   | Refers to the name of the application. Used to namespace queues and metrics.                |
| **nrepl-server**                   | `Integer`  | Port on which the REPL server will be hosted.                                               |
| **default-api-timeout-ms-config**  | `Integer`  | Specifies the timeout (in milliseconds) for client APIs. Recommended value is 600000 ms.    |

## Stream Router Configurations

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **stream-router**                  | `Object`   | Configs related to all the Kafka streams the application is reading from.                   |

### Stream Router Properties

| Property                           | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **stream-id**                      | `String`   | The identifier of a stream mentioned in `main.clj`. Each stream can read from different Kafka brokers and have different threads. |
| **application-id**                 | `String`   | The Kafka consumer group id. [Documentation](https://kafka.apache.org/intro#intro_consumers) |
| **bootstrap-servers**              | `String`   | The Kafka brokers that the application will read from. Accepts a comma-separated value.     |
| **stream-threads-count**           | `Integer`  | Number of parallel threads to read messages from Kafka. Can scale up to the number of partitions. |
| **stream-thread-exception-response** | `String`   | Action triggered on an uncaught exception. Possible values: `:shutdown-client` (default), `:shutdown-application`, `:replace-thread`. [More info](https://kafka-tutorials.confluent.io/error-handling/kstreams.html?_ga=2.107379330.1454767099.1620795696-1044723812.1563788148) |
| **origin-topic**                   | `String`   | The topic that the stream should read from. Can be a regex. Messages from different streams will be passed to the same mapper-function. |
| **oldest-processed-messages-in-s** | `Integer`  | Oldest message processed by the stream in seconds. Default value is 604800 (1 week).        |
| **changelog-topic-replication-factor** | `Integer`  | Internal changelog topic replication factor. Default value is 3.                           |

### Channels

| Property                           | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **worker-count**                   | `Integer`  | Number of messages to process in parallel per channel.                                      |
| **retry**                          | `Object`   | Defines channel retries.                                                                   |

#### Retry Properties

| Property                           | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **type**                           | `String`   | Type of retry (linear, exponential).                                                        |
| **count**                          | `Integer`  | Number of retries before message is sent to channel DLQ.                                    |
| **enabled**                        | `Boolean`  | If channel retries are enabled or not.                                                      |

### Producer

| Property                           | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **bootstrap.servers**              | `String`   | List of host/port pairs to use for establishing the initial connection to the Kafka cluster.|
| **acks**                           | `String`   | Number of acknowledgments the producer requires before considering a request complete. Valid values: [all, -1, 0, 1]. |
| **retries**                        | `Integer`  | Number of retries for any record whose send fails with a potentially transient error.       |
| **key.serializer**                 | `String`   | Serializer class for key implementing the `org.apache.kafka.common.serialization.Serializer` interface. |
| **value.serializer**               | `String`   | Serializer class for value implementing the `org.apache.kafka.common.serialization.Serializer` interface. |
| **max.in.flight.requests.per.connection** | `Integer`  | Maximum number of unacknowledged requests the client will send on a single connection before blocking. |
| **enable.idempotence**             | `Boolean`  | Ensures that exactly one copy of each message is written in the stream if set to `true`.    |

## Batch Routes

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **batch-routes**                   | `Object`   | Properties provided with [Kafka Consumer Config](https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html) are accepted as kebab case keywords. |

## SSL

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **ssl**                            | `Object`   | All Kafka [SSL configs](https://kafka.apache.org/28/javadoc/org/apache/kafka/common/config/SslConfigs.html) and [SASL configs](https://kafka.apache.org/28/javadoc/org/apache/kafka/common/config/SaslConfigs.html) can be provided as kebab case keywords. Automatically applied to all Kafka stream, Kafka producer, and Kafka consumer objects created in Ziggurat. |

## StatsD

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **statsd**                         | `Object`   | Formerly known as Datadog, the StatsD host and port that metrics should be sent to.         |

## Sentry

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **sentry**                         | `Object`   | Sends an event to Sentry when a `:failure` keyword is returned from the mapper-function or an exception is raised. Can be disabled. |

## RabbitMQ Connection

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **rabbit-mq-connection**           | `Object`   | Details required to make a connection to RabbitMQ. Used for the retry mechanism.            |

## RabbitMQ

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **rabbit-mq**                      | `Object`   | The queues that are part of the retry mechanism.                                            |

## Retry

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **retry**                          | `Object`   | Number of times the message should be retried and if retry flow should be enabled. If retry is disabled, and `:retry` is returned from mapper function, messages will be lost. |

## Jobs

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **jobs**                           | `Object`   | Number of consumers that should be reading from the retry queues and the prefetch count of each consumer. |

## HTTP Server

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **http-server**                    | `Object`   | Defines the port and number of threads for the HTTP server. Also controls the graceful shutdown timeout. Default is `30000ms`. |

## New Relic

| Configuration                      | Data Type  | Description                                                                                 |
|------------------------------------|------------|---------------------------------------------------------------------------------------------|
| **new-relic**                      | `Object`   | If `report-errors` is true, reports an error to New Relic whenever a `:failure` keyword is returned from the mapper-function or an exception is raised. Can be disabled. |
