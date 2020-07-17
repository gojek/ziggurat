# Ziggurat

<p align="center">
  <img src="https://github.com/gojektech/ziggurat/wiki/images/logo-ziggurat.png">
</p>

<p align="center">
  <a href="https://travis-ci.com/gojek/ziggurat">
    <img src="https://travis-ci.com/gojek/ziggurat.svg?branch=master" alt="Build Status" />
  </a>
  <a href='https://coveralls.io/github/gojek/ziggurat?branch=master'>
    <img src='https://coveralls.io/repos/github/gojek/ziggurat/badge.svg?branch=master' alt='Coverage Status' />
  </a>
  <a href='https://clojars.org/tech.gojek/ziggurat'>
    <img src='https://img.shields.io/clojars/v/tech.gojek/ziggurat.svg' alt='Clojars Project' />
  </a>
</p>

* [Wiki](https://github.com/gojek/ziggurat/wiki)
* [Release Notes](https://github.com/gojek/ziggurat/wiki/Release-Notes)
* [Upgrade Guide](https://github.com/gojek/ziggurat/wiki/Upgrade-guide)
-----------------------------------------------------------------------------------------
* [Description](#description)
* [Dev Setup](#dev-setup)
* [Usage](#usage)
* [Configuration](#configuration)
* [Contribution Guidelines](#contribution)
* [License](#license)


## Description

Ziggurat is a framework built to simplify Stream processing on Kafka. It can be used to create a full-fledged Clojure app that reads and processes messages from Kafka.
Ziggurat is built with the intent to abstract out
```
- reading messages from Kafka
- retrying failed messages
- setting up an HTTP server
```
from a clojure application such that a user only needs to pass a function that will be mapped to every message recieved from Kafka.

Refer [concepts](doc/CONCEPTS.md) to understand the concepts referred to in this document.

## Dev Setup
(For mac users only)

- Install Clojure: ```brew install clojure```

- Install leiningen: ```brew install leiningen```

- Run docker-compose: ```docker-compose up```. This starts
    - Kafka on localhost:9092
    - ZooKeeper on localhost:2181
    - RabbitMQ on localhost:5672

- Run tests: ```make test```


## Usage

Add this to your project.clj

`[tech.gojek/ziggurat "3.1.0"]`

_Please refer [clojars](https://clojars.org/tech.gojek/ziggurat) for the latest stable version_

To start a stream (a thread that reads messages from Kafka), add this to your core namespace.
```clojure
(require '[ziggurat.init :as ziggurat])

(defn start-fn []
    ;; your logic that runs at startup goes here
)

(defn stop-fn []
    ;; your logic that runs at shutdown goes here
)

(defn main-fn
    [message]
    (println message)
    :success)

(def handler-fn
    (-> main-fn
      (middleware/protobuf->hash ProtoClass :stream-id)))
;; Here ProtoClass refers to the fully qualified name of the Java class which the code is used to de-serialize the message.

(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn handler-fn}})
```
_NOTE: this example assumes that the message is serialized in Protobuf format_

Please refer the [Middleware section](#middleware-in-ziggurat) for understanding `handler-fn` here.

* The main-fn is the function that will be applied to every message that is read from the Kafka stream.
* The main-fn returns a keyword which can be any of the below words
    * :success - The message was successfuly processed and the stream should continue to the next message
    * :retry - The message failed to be processed and it should be retried.
    * :skip - The message should be skipped without reporting its failure or retrying the message
* The start-fn is run at the application startup and can be used to initialize connection to databases, http clients, thread-pools, etc.
* The stop-fn is run at shutdown and facilitates graceful shutdown, for example, releasing db connections, shutting down http servers etc.
* Ziggurat enables reading from multiple streams and applying same/different functions to the messages. `:stream-id` is a unique identifier per stream. All configs, queues and metrics will be namespaced under this id.

```clojure
(ziggurat/main start-fn stop-fn {:stream-id-1 {:handler-fn handler-fn-1}
                                         :stream-id-2 {:handler-fn handler-fn-2}})
```

```clojure
(require '[ziggurat.init :as ziggurat])

(defn start-fn []
    ;; your logic that runs at startup goes here
)

(defn stop-fn []
    ;; your logic that runs at shutdown goes here
)


(defn api-handler [_request]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (get-resource)})

(def routes [["v1/resources" {:get api-handler}]])

(defn main-fn
    [message]
    (println message)
    :success)

(def handler-fn
    (-> main-fn
      (middleware/protobuf->hash ProtoClass :stream-id)))

(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn handler-fn}} routes)

```
_NOTE: this example assumes that the message is serialized in Protobuf format_

Ziggurat also sets up a HTTP server by default and you can pass in your own routes that it will serve. The above example demonstrates
how you can pass in your own route.

or
```clojure
(ziggurat/main {:start-fn start-fn
                :stop-fn stop-fn
                :stream-routes {:stream-id {:handler-fn main-fn}}
                :actor-routes routes
                :modes [:api-server :stream-worker]})
```

This will start both api-server and stream-worker modes

There are four modes supported by ziggurat
```
 :api-server - Mode by which only server will be started with actor routes and management routes(Dead set management)
 :stream-worker - Only start the server plus rabbitmq for only producing the messages for retry and channels
 :worker - Starts the rabbitmq consumer for retry and channel
 :management-api - Servers only routes which used for deadset management
```

You can pass in multiple modes and it will start accordingly
If nothing passed to modes then it will start all the modes.

## Middleware in Ziggurat
Version 3.0.0 of Ziggurat introduces the support of Middleware. Old versions of Ziggurat (< 3.0) assumed that the messages read from kafka were serialized in proto-format and thus it deserialized
them and passed a clojure map to the mapper-fn. We have now pulled the deserialization function into a middleware and users have the freedom to use this function to deserialize their messages
or define their custom middlewares. This enables ziggurat to process messages serialized in any format.

### Custom Middleware usage
The default middleware `default/protobuf->hash` assumes that the message is serialized in proto format.
```clojure
(require '[ziggurat.init :as ziggurat])

(defn start-fn []
    ;; your logic that runs at startup goes here
)

(defn stop-fn []
    ;; your logic that runs at shutdown goes here
)

(defn main-fn
    [message]
    (println message)
    :success)

(defn wrap-middleware-fn
    [mapper-fn :stream-id]
    (fn [message]
      (println "processing message for stream: " :stream-id)
      (mapper-fn (deserialize-message message))))

(def handler-fn
    (-> main-fn
      (wrap-middleware-fn :stream-id)))

(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn handler-fn}})
```
_The handler-fn gets a serialized message from kafka and thus we need a deserealize-message function. We have provided default deserializers in Ziggurat_

### Deserializing JSON messages using JSON middleware
Ziggurat 3.1.0 provides a middleware to deserialize JSON messages, along with proto.
It can be used like this.

```clojure
(def message-handler-fn
  (-> actual-message-handler-function
      (parse-json :stream-route-config)))
```

Here, `message-handler-fn` calls `parse-json` with a message handler function
`actual-message-handler-function` as the first argument and the key of a stream-route
config (as defined in `config.edn`) as the second argument.

## Publishing data to Kafka Topics in Ziggurat
To enable publishing data to kafka, Ziggurat provides producing support through ziggurat.producer namespace. This namespace defines methods for publishing data to Kafka topics. The methods defined here are essentially wrapper around variants of `send` methods defined in `org.apache.kafka.clients.producer.KafkaProducer`.

At the time of initialization, an instance of `org.apache.kafka.clients.producer.KafkaProducer` is constructed using config values provided in `resources/config.edn`. A producer can be configured for each of the stream-routes in config.edn. Please see the example below.

At present, only a few configurations are supported for constructing KafkaProducer. These have been explained [here](#configuration). Please see [Producer configs](http://kafka.apache.org/documentation.html#producerconfigs)
for a complete list of all producer configs available in Kafka.

Ziggurat.producer namespace defines a multi-arity `send` function which is a thin wrapper around [KafkaProducer#send](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send-org.apache.kafka.clients.producer.ProducerRecord-). This method publishes data to a Kafka topic through a Kafka producer
defined in the stream router configuration. See configuration section below.

E.g.
For publishing data using a producer which is defined for the stream router config with key `:default`, use send like this:

`(send :default "test-topic" "key" "value")`

`(send :default "test-topic" 1 "key" "value")`

## Tracing
[Open Tracing](https://opentracing.io/docs/overview/) enables to identify the amount of time spent in various stages of the work flow.

Currently, the execution of the handler function is traced. If the message consumed has the corresponding tracing headers, then the E2E life time of the message from the time of production till the time of consumption can be traced.

Tracing has been added to the following flows:

1. Normal basic consume
2. Retry via rabbitmq
3. Produce to rabbitmq channel
4. Produce to another kafka topic

By default, tracing is done via [Jaeger](https://www.jaegertracing.io/) based on the env configs. Please refer [Jaeger Configuration](https://github.com/jaegertracing/jaeger-client-java/tree/master/jaeger-core#configuration-via-environment)
and [Jaeger Architecture](https://www.jaegertracing.io/docs/1.13/architecture/) to set the respective env variables.
To enable custom tracer, a custom tracer provider function name can be set in `:custom-provider`. The corresponding function will be executed in runtime to create a tracer. In the event of any errors while executing the custom tracer provider, a Noop tracer will be created.

To enable tracing, the following config needs to be added to the `config.edn` under `:ziggurat` key.

```clojure
:tracer {:enabled               [true :bool]
         :custom-provider       ""}
```

Example Jaeger Env Config:
```
JAEGER_SERVICE_NAME: "service-name"
JAEGER_AGENT_HOST: "localhost"
JAEGER_AGENT_PORT: 6831
```

## Stream Joins
This will allow an actor to join messages from 2 topics into 1 result. To be able to use stream joins just add the configuration below to your `config.edn`
```clojure
{:ziggurat  {:stream-router        {:stream-id            {
    :consumer-type        :stream-joins
    :input-topics         {:topic-1-key {:name "topic-1"} :topic-2-key {:name "topic-2"}}
    :join-cfg             {:topic-1-and-topic-2 {:join-window-ms 5000 :join-type :inner}}
}}}}
```
* consumer-type - enables stream joins if `:stream-joins` key is provided, other possible value is `:default` which is the default actor behavior
* input-topics - a map of topics in which you want to use for joining
* join-cfg - a map of configurations which you define the join-window-ms and the join-type (`:inner`, `:left` or `:outer`)

And your actor's handler function be like
```clojure
(ns my-actor
  (:require [ziggurat.middleware.stream-joins :as mw]))

(def handler-func
  (-> main-func
      (mw/protobuf->hash [com.gojek.esb.booking.BookingLogMessage com.gojek.esb.booking.BookingLogMessage] :booking)))
```

`Please take note of the vector containing the proto classes`

Your handler function will receive a message in the following format/structure

```clojure
{:topic-1-key "message-from-1st-topic" :topic-2-key "message-from-2nd-topic"}
```

## Connecting to a RabbitMQ cluster (alpha feature)
* To connect to RabbitMQ clusters add the following config to your `config.edn`
```clojure
{:ziggurat {:messaging {:constructor "ziggurat.messaging.rabbitmq-cluster-wrapper/->RabbitMQMessaging"
            :rabbit-mq-connection {:hosts "g-lambda-lambda-rabbitmq-a-01,g-lambda-lambda-rabbitmq-a-02,g-lambda-lambda-rabbitmq-a-03"
                                   :port [5672 :int]
                                   :prefetch-count  [3 :int]
                                   :username        "guest"
                                   :password        "guest"
                                   :ha-mode         "all"
                                   :admin-port      [15672 :int]
                                   :ha-sync-mode    "automatic"
                                   :channel-timeout [2000 :int]}}}}
```
* `:hosts` is a comma separated values of RabbitMQ hostnames (dns-names OR IPs).
* `:port` specifies the port number on which the RabbitMQ nodes are running.
* `:ha-mode` is set to `"all"`.
* `:ha-sync-mode` can be set to `"automatic"` OR `"manual"`.
* `:admin-port` is the port on which the RabbitMQ management UI is exposed, defaults to `15672`.


## Configuration

All Ziggurat configs should be in your `clonfig` `config.edn` under the `:ziggurat` key.
```clojure
{:ziggurat  {:app-name            "application_name"
            :nrepl-server         {:port [7011 :int]}
            :stream-router        {:stream-id            {:application-id                 "kafka_consumer_id"
                                                          :bootstrap-servers              "kafka-broker-1:6667,Kafka-broker-2:6667"
                                                          :stream-threads-count           [1 :int]
                                                          :origin-topic                   "kafka-topic-*"
                                                          :oldest-processed-message-in-s  [604800 :int]
                                                          :changelog-topic-replication-factor [3 :int]
                                                          :producer   {:bootstrap-servers                     "localhost:9092"
                                                                       :acks                                  "all"
                                                                       :retries-config                        5
                                                                       :max-in-flight-requests-per-connection 5
                                                                       :enable-idempotence                    false
                                                                       :value-serializer                      "org.apache.kafka.common.serialization.StringSerializer"
                                                                       :key-serializer                        "org.apache.kafka.common.serialization.StringSerializer"}}}
            :datadog              {:host    "localhost"
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
            :retry                {:count   [5 :int]
                                   :enabled [false :bool]}
            :jobs                 {:instant {:worker-count   [4 :int]
                                             :prefetch-count [4 :int]}}
            :http-server          {:port         [8010 :int]
                                               :thread-count [100 :int]}}}
```
* app-name - Refers to the name of the application. Used to namespace queues and metrics.
* nrepl-server - Port on which the repl server will be hosted
* stream-router - Configs related to all the Kafka streams the application is reading from
    * stream-id - the identifier of a stream that was mentioned in main.clj. Hence each stream can read from different Kafka brokers and have different number of threads (depending on the throughput of the stream).
        * application-id - The Kafka consumer group id. [Documentation](https://kafka.apache.org/intro#intro_consumers)
        * bootstrap-servers - The Kafka brokers that the application will read from. It accepts a comma seperated value.
        * stream-threads-count - The number of parallel threads that should read messages from Kafka. This can scale up to the number of partitions on the topic you wish to read from.
        * origin-topic - The topic that the stream should read from. This can be a regex that enables you to read from multiple streams and handle the messages in the same way. It is to be kept in mind that the messages from different streams will be passed to the same mapper-function.
        * oldest-processed-messages-in-s - The oldest message which will be processed by stream in second. By default the value is 604800 (1 week)
        * changelog-topic-replication-factor - the internal changelog topic replication factor. By default the value is 3
        * producer - Configuration for KafkaProducer. Currently, only following options are supported. Please see [Producer Configs](https://kafka.apache.org/documentation/#producerconfigs) for detailed explanation for each of the configuration parameters.
            * bootstrap.servers - A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
            * acks - The number of acknowledgments the producer requires the leader to have received before considering a request complete. Valid values are [all, -1, 0, 1].
            * retries - Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.
            * key.serializer - Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface.
            * value.serializer - Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface.
            * max.in.flight.requests.per.connection - The maximum number of unacknowledged requests the client will send on a single connection before blocking.
            * enable.idempotence - When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer retries due to broker failures, etc., may write duplicates of the retried message in the stream.

* datadog - The statsd host and port that metrics should be sent to, although the key name is datadog, it supports statsd as well to send metrics.
* statsd - Same as datadog but with a more appropriate name, the :datadog key will be deprecated in the future.
* sentry - Whenever a :failure keyword is returned from the mapper-function or an exception is raised while executing the mapper-function, an event is sent to sentry. You can skip this flow by disabling it.
* rabbit-mq-connection - The details required to make a connection to rabbitmq. We use rabbitmq for the retry mechanism.
* rabbit-mq - The queues that are part of the retry mechanism
* retry - The number of times the message should be retried and if retry flow should be enabled or not
* jobs - The number of consumers that should be reading from the retry queues and the prefetch count of each consumer
* http-server - Ziggurat starts an http server by default and gives apis for ping health-check and deadset management. This defines the port and the number of threads of the http server.

## Alpha (Experimental) Features
The contract and interface for experimental features in Ziggurat can be changed as we iterate towards better designs for that feature.
For all purposes these features should be considered unstable and should only be used after understanding their risks and implementations.

### Exponential Backoff based Retries
In addition to linear retries, Ziggurat users can now use exponential backoff strategy for retries. This means that the message
timeouts after every retry increase by a factor of 2. So, if your configured timeout is 100ms the backoffs will have timeouts as
`200, 300, 700, 1500 ..`. These timeouts are calculated using the formula `(queue-timeout-ms * ((2**exponent) - 1))` where `exponent` falls in this range `[1,(min 25, configured-retry-count)]`.

The number of retries possible in this case are capped at 25. The number of queues created in the RabbitMQ are equal to the configured-retry-count or 25, whichever is smaller.

Exponential retries can be configured as described below.

```$xslt
:ziggurat {:stream-router {:default {:application-id "application_name"...}}}
           :retry         {:type   [:exponential :keyword]
                           :count  [10 :int]
                           :enable [true :bool]}

```

Exponential retries can be configured for channels too. Additionally, a user can specify a custom `queue-timeout-ms` value per channel.
Timeouts for exponential backoffs are calculated using `queue-timeout-ms`. This implies that each channel can have separate count of retries
and different timeout values.

```$xslt
:ziggurat {:stream-router {:default {:application-id "application_name"...
                                     :channels {:channel-1 .....
                                                           :retry {:type   [:exponential :keyword]
                                                                                      :count  [10 :int]
                                                                                      :queue-timeout-ms 2000
                                                                                      :enable [true :bool]}}}}}
```

## Deprecation Notice
- Please note that the :datadog key inside the config file will be removed (sometime in the future) in favor of :statsd. Both contents are thesame though, it's just the key name was changed.
  The reason for this is to avoid confusion with our users. We would still have backward compatibility for the :datadog key.

## Contribution
- For dev setup and contributions please refer to CONTRIBUTING.md

## License
```
Copyright 2018, GO-JEK Tech <http://gojek.tech>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
