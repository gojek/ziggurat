## Publishing data to Kafka Topics using Kafka Producer API

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


## Batch Consumption using Kafka Consumer API
With Ziggurat version 3.5.1, both Kafka Streams API and Kafka Consumer API can be used to consume the messages in real
time. Kafka Consumer API is an efficient way to consume messages from high throughput Kafka topics.

With Kafka Streams API, one message is processed at a time. But, with Kafka Consumer API integration in Ziggurat,
a user can consume messages in bulk and can control how many messages it wants to consume at a time. This batch size
can be configured using max-poll-records config
https://docs.confluent.io/current/installation/configuration/consumer-configs.html#max.poll.records.

Like Streams, Ziggurat also provides the facility to specify multiple batch routes.

### How to enable batch consumption in an actor?

##### Changes required in config.edn
```clojure
:batch-routes {:restaurants-updates-to-non-personalized-es 
                {:consumer-group-id          "restaurants-updates-consumer"
                 :bootstrap-servers          "g-gojek-id-mainstream.golabs.io:6668"
                 :origin-topic               "restaurant-updates-stream"}}
```
A full list of supported configs is given below. These configs can be added to `config.edn` as per the requirements.

##### Call to Ziggurat Init Function
```clojure
(defn -main [& args]
  (init/main {:start-fn      start
              :stop-fn       stop
              :stream-routes {:booking {:handler-fn (stream-deserializer/protobuf->hash
                                                      stream-handler
                                                      BookingLogMessage
                                                      :booking)}}
              :batch-routes  {:batch-consumer-1 {:handler-fn (batch-deserialzer/deserialize-batch-of-proto-messages
                                                               batch-handler
                                                               BookingLogKey
                                                               BookingLogMessage
                                                               :batch-consumer-1)}}
              :actor-routes  [["v1/hello" {:get get-hello}]]}))
```

##### The Batch Handler
```clojure
(defn- single-message-handler
  [message]
  (log/info "Batch Message: " message))

(defn batch-handler
  [batch]
  (log/infof "Received a batch of %d messages" (count batch))
  (doseq [single-message batch]
    (single-message-handler single-message))
  (if (retry?)
    (do (log/info "Retrying the batch..")
        {:retry batch :skip []})
    {:retry [] :skip []}))
```

##### List of all the supported configs for Batch Consumption
Ziggurat Config | Default Value | Description | Mandatory?
--- | --- | --- | ---
:bootstrap-servers | NA | [https://kafka.apache.org/documentation/#bootstrap.servers](https://kafka.apache.org/documentation/#bootstrap.servers) | Yes
:consumer-group-id | NA | [https://kafka.apache.org/documentation/#group.id](https://kafka.apache.org/documentation/#group.id)                   | Yes
:origin-topic      | NA | Kafka Topic to read data from                                                                                          | Yes
:max-poll-records  | 500 | [https://kafka.apache.org/documentation/#max.poll.records](https://kafka.apache.org/documentation/#max.poll.records)  | No
:session-timeout-ms-config | 60000 | [https://kafka.apache.org/documentation/#session.timeout.ms](https://kafka.apache.org/documentation/#session.timeout.ms) | No
:key-deserializer-class-config | "org.apache.kafka.common.serialization.ByteArrayDeserializer" | [https://kafka.apache.org/documentation/#key.deserializer](https://kafka.apache.org/documentation/#key.deserializer) | No
:value-deserializer-class-config | "org.apache.kafka.common.serialization.ByteArrayDeserializer" | [https://kafka.apache.org/documentation/#value.deserializer](https://kafka.apache.org/documentation/#value.deserializer) | No
:poll-timeout-ms-config | 1000 | [Timeout value used for polling with a Kafka Consumer](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java#L1160) | No
:thread-count | 2 | Number of Kafka Consumer threads for each batch-route | No
:default-api-timeout-ms | 60000 | [https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior](https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior) | No


## Connecting to a RabbitMQ cluster for retries

- To connect to RabbitMQ clusters add the following config to your `config.edn`

```clojure
{:ziggurat {:messaging {:constructor "ziggurat.messaging.rabbitmq-cluster-wrapper/->RabbitMQMessaging"
            :rabbit-mq-connection {:hosts "g-lambda-lambda-rabbitmq-a-01,g-lambda-lambda-rabbitmq-a-02,g-lambda-lambda-rabbitmq-a-03"
                                   :port [5672 :int]
                                   :prefetch-count  [3 :int]
                                   :username        "guest"
                                   :password        "guest"
                                   :channel-timeout [2000 :int]
                                   :address-resolver [:dns :keyword] ;;possible values [:dns :ip-list]. Default is :dns
                                   :channel-pool     {:max-wait-ms [5000 :int]
                                                      :min-idle    [10 :int]
                                                      :max-idle    [20 :int]}
                                   :publish-retry   {:back-off-ms 5000
                                                     :non-recoverable-exception {:enabled true
                                                                                 :back-off-ms 1000
                                                                                 :count 5}}}}}}
```

- `:hosts` is a comma separated values of RabbitMQ hostnames (dns-names OR IPs).
- `:port` specifies the port number on which the RabbitMQ nodes are running.
- `:prefetch-count` Sets the prefetch count for RabbitMQ, determining the number of messages that can be consumed from a channel before an acknowledgment is received. 
  - The value 3 means up to 3 messages can be prefetched.
  - The prefetch count is per worker. so, the prefetch-count mentioned here is for each worker in worker-count.
- `:channel-pool` specifies the properties for the RabbitMQ channel pool used for publishing
- `:address-resolver` specifies the strategy to figure out RabbitMQ hosts IP addresses. `:dns` is the default and shoud
  be used when `:hosts` specifies a DNS address. `:ip-list` should be used when comma separated IPs are provided.
- `:publish-retry` defines the config for recoverable and non-recoverable exceptions.
    - Recoverable exceptions
        - `:back-off-ms` - defines the time period after which a retry should happen
    - Non-recoverable exceptions
        - `:enabled` - defines whether retries should happen
        - `:back-off-ms` - defines the time period after which a retry should happen
        - `:count` - defines the number of retries
- By default, your queues and exchanges are replicated across (n+1)/2 nodes in the cluster

## Exponential Backoff based Retries

In addition to linear retries, Ziggurat users can now use exponential backoff strategy for retries. This means that the message
timeouts after every retry increase by a factor of 2. So, if your configured timeout is 100ms the backoffs will have timeouts as
`200, 300, 700, 1500 ..`. These timeouts are calculated using the formula `(queue-timeout-ms * ((2**exponent) - 1))` where `exponent` falls in this range `[1,(min 25, configured-retry-count)]`.

The number of retries possible in this case are capped at 25.

The number of queues created in the RabbitMQ are equal to the configured-retry-count or 25, whichever is smaller.

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

## Channels

Channels enable you to increase the number of parallel processors more than the number of partitions of your topic.
Messages consumed from topics are directly sent to rabbitmq channels and the mapper function handler processes messages from this channel.
You can set the worker count as per your parallel processing requirement. The channel configs are described below in configuration section.

```$xslt
:ziggurat {:stream-router {:stream-id {:application-id "application_name"...
                                     :channels {:channel-1 {:worker-count [10 :int]
                                                            :retry        {:type    [:linear :keyword]
                                                                           :count   [5 :int]
                                                                           :enabled [true :bool]}}}
```

How to send messages directly to channel after consuming.
```clojure
:stream-routes {:stream-id {:handler-fn   (fn [_] :channel-1)
                               :channel-1 (my-protobuf-hash handler-fn)}}

```

The above method creates an anonymous function that passes the consumed messages to channel and the channel route is then handled by the handler-fn
you have created.