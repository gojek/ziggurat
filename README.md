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

* [Description](#description)
* [Dev Setup](#dev-setup)
* [Usage](#usage)
* [Configuration](#configuration)
* [Contribution Guidelines](#contribution)
* [License](#license)

## Description

Ziggurat is a framework built to simplify Multi-Stream processing on Kafka. It can be used to create a full-fledged Clojure app that reads and processes messages from Kafka.
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
`[tech.gojek/ziggurat "2.9.1"]`

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
  
(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn main-fn}})
```

* The main-fn is the function that will be applied to every message that is read from the Kafka stream.
* The main-fn returns a keyword which can be any of the below words
    * :success - The message was successfuly processed and the stream should continue to the next message
    * :failure - The message failed to be processed and it should be reported to sentry, it will not be retried.
    * :retry - The message failed to be processed and it should be retried.
    * :skip - The message should be skipped without reporting it's failure or retrying the message
* The start-fn is run at the application startup and can be used to initialize connection to databases, http clients, thread-pools, etc.
* The stop-fn is run at shutdown and facilitates graceful shutdown, for example, releasing db connections, shutting down http servers etc.
* Ziggurat enables reading from multiple streams and applying same/different functions to the messages. `:stream-id` is a unique identifier per stream. All configs, queues and metrics will be namespaced under this id.

    * ```clojure
        (ziggurat/main start-fn stop-fn {:stream-id-1 {:handler-fn main-fn-1}
                                         :stream-id-2 {:handler-fn main-fn-2}})
        ```

```clojure
(require '[ziggurat.init :as ziggurat])

(defn start-fn []
    ;; your logic that runs at startup goes here
)
  
(defn stop-fn []
    ;; your logic that runs at shutdown goes here
)
  
  
(defn handler-function [_request]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (get-resource)})
   
(def routes [["v1/resources" {:get handler-function}]])
  
(defn main-fn
    [message]
    (println message)
    :success)
  
(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn main-fn}} routes)

```
Ziggurat also sets up a HTTP server by default and you can pass in your own routes that it will serve. The above example demonstrates
how you can pass in your own route. 



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
                                                          :proto-class          "proto-class"
                                                          :changelog-topic-replication-factor [3 :int]}}
            :datadog              {:host    "localhost"
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
        * origin-topic - The topic that the stream should read from. This can be a regex that enables you to read from multiple streams and handle the messages in the same way. It is to be kept in mind that the messages from different streams should be of the same proto-class.
        * oldest-processed-messages-in-s - The oldest message which will be processed by stream in second. By default the value is 604800 (1 week)
        * proto-class - The proto-class of the message so that it can be decompiled before being passed to the mapper function
        * changelog-topic-replication-factor - the internal changelog topic replication factor. By default the value is 3
* datadog - The statsd host and port that metrics should be sent to, although the key name is datadog, it supports statsd as well to send metrics.
* sentry - Whenever a :failure keyword is returned from the mapper-function or an exception is raised while executing the mapper-function, an event is sent to sentry. You can skip this flow by disabling it.
* rabbit-mq-connection - The details required to make a connection to rabbitmq. We use rabbitmq for the retry mechanism.
* rabbit-mq - The queues that are part of the retry mechanism
* retry - The number of times the message should be retried and if retry flow should be enabled or not
* jobs - The number of consumers that should be reading from the retry queues and the prefetch count of each consumer
* http-server - Ziggurat starts an http server by default and gives apis for ping health-check and deadset management. This defines the port and the number of threads of the http server.

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
