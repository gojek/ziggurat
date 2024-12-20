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

## Table of Contents

- [Wiki](https://github.com/gojek/ziggurat/wiki)
- [Release Notes](https://github.com/gojek/ziggurat/wiki/Release-Notes)
- [Upgrade Guide](https://github.com/gojek/ziggurat/wiki/Upgrade-guide)
- [Description](#description)
- [Dev Setup](#dev-setup)
- [Usage](#usage)
- [Configuration](doc/configuration.md)
- [Contribution Guidelines](#contribution)
- [License](#license)
- [Changelog](CHANGELOG.md)

## Description

Ziggurat is a framework built to simplify stream processing on Kafka. It can be used to create a full-fledged Clojure app that reads and processes messages from Kafka. Ziggurat abstracts the following features:

- Reading messages from Kafka
- Retrying failed messages via RabbitMQ
- Setting up an HTTP server

Refer to [concepts](doc/CONCEPTS.md) to understand the concepts referred to in this document.

### Important Concepts and Usage Docs

- [Ziggurat HTTP Server](doc/CONCEPTS.md#Http-Server)
- [Toggle Streams on a Running Actor](doc/CONCEPTS.md#toggle-streams-in-running-actor)
- [Middlewares in Ziggurat](doc/middleware.md)
- [Consuming and Publishing Messages to Kafka](doc/kafka_produce_consume.md)
- [Connecting RabbitMQ and using channels](doc/rmq_channels.md)
- [Configuration and Config Description](doc/configuration.md)
- To read about all concepts, please refer the [Concepts file](doc/CONCEPTS.md)

## Dev Setup

### For Mac Users Only

1. Install Clojure: `brew install clojure`
2. Install Leiningen: `brew install leiningen`
3. Run docker-compose: `docker-compose up`. This starts:
  - Kafka on localhost:9092
  - ZooKeeper on localhost:2181
  - RabbitMQ on localhost:5672
4. Run tests: `make test`

### Running a Cluster Setup Locally

- Run `make setup-cluster`. This clears up the volume and starts:
  - 3 Kafka brokers on localhost:9091, localhost:9092, and localhost:9093
  - Zookeeper on localhost:2181
  - RabbitMQ on localhost:5672

### Running Tests via a Cluster

- Run `make test-cluster`. This uses `config.test.cluster.edn` instead of `config.test.edn`.

## Usage

Add this to your `project.clj`:

```clojure
[tech.gojek/ziggurat "4.11.1"]


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
  [{:keys [message metadata] :as message-payload}]
    (println message)
    :success)

(def handler-fn
    (-> main-fn
      (middleware/protobuf->hash ProtoClass :stream-id)))
;; Here ProtoClass refers to the fully qualified name of the Java class which the code is used to de-serialize the message.

(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn handler-fn}})
```

_NOTE: this example assumes that the message is serialized in Protobuf format_

Please refer the [Middleware section](doc/middleware.md) for understanding `handler-fn` here.

- The main-fn is the function that will be applied to every message that is read from the Kafka stream.
- The main-fn will take map as an argument that takes 2 keys i.e
  - message - It is the byte[] array received from kafka.
  - metadata
    - topic - It is the topic from where kafka message is consumed.
    - timestamp - It is ingestion timestamp in kafka.
    - partition - The partition from message is consumed.
    - rabbitmq-retry-count - The number of retries done by rabbitmq for given message.
- The main-fn returns a keyword which can be any of the below words
  - :success - The message was successfully processed and the stream should continue to the next message
  - :retry - The message failed to be processed and it should be retried via RabbitMQ.
  - :dead-letter - The message is not retried and is directly pushed to the dead letter queue
  - :skip - The message should be skipped without reporting its failure or retrying the message. Same as :success except that a different metric is published to track skipped messages
- The start-fn is run at the application startup and can be used to initialize connection to databases, http clients, thread-pools, etc.
- The stop-fn is run at shutdown and facilitates graceful shutdown, for example, releasing db connections, shutting down http servers etc.



## Multiple stream routes
- Ziggurat enables reading from multiple streams and applying same/different functions to the messages. `:stream-id` is a unique identifier per stream which needs to be included in config.edn file
- All configs, queues and metrics will be namespaced under this id.

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
  [{:keys [message metadata] :as message-payload}]
    (println message)
    :success)

(def handler-fn
    (-> main-fn
      (middleware/protobuf->hash ProtoClass :stream-id)))

(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn handler-fn}} routes)

```

_NOTE: this example assumes that the message is serialized in Protobuf format_


## Deprecation Notice
* Sentry has been deprecated from version 4.6.3. 

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
