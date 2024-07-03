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

