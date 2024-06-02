## Configuration

As of Ziggurat version 3.13.0, all the official Kafka configs Kafka configurations for [Streams API](https://docs.confluent.io/platform/current/installation/configuration/streams-configs.html), [Consumer API](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html) and [Producer API](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html) are supported.

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

- app-name - Refers to the name of the application. Used to namespace queues and metrics.
- nrepl-server - Port on which the repl server will be hosted
- default-api-timeout-ms-config - Specifies the timeout (in milliseconds) for client APIs. This configuration is used as the default timeout for all client operations that do not specify a timeout parameter. The recommended value for Ziggurat based apps is 600000 ms (10 minutes).
- stream-router - Configs related to all the Kafka streams the application is reading from

    - stream-id - the identifier of a stream that was mentioned in main.clj. Hence each stream can read from different Kafka brokers and have different number of threads (depending on the throughput of the stream). A stream-id accepts all the properties (as kebab case keywords) provided by [Kafka Streams API](https://kafka.apache.org/28/javadoc/org/apache/kafka/streams/StreamsConfig.html).
        - application-id - The Kafka consumer group id. [Documentation](https://kafka.apache.org/intro#intro_consumers)
        - bootstrap-servers - The Kafka brokers that the application will read from. It accepts a comma seperated value.
        - stream-threads-count - The number of parallel threads that should read messages from Kafka. This can scale up to the number of partitions on the topic you wish to read from.
        - stream-thread-exception-response - This describes what particular action will be triggered if an uncaught exception is encountered. Possible values are :shutdown-client (default), :shutdowm-application and :replace-thread. The 3 responses are documented [here](https://kafka-tutorials.confluent.io/error-handling/kstreams.html?_ga=2.107379330.1454767099.1620795696-1044723812.1563788148).
        - origin-topic - The topic that the stream should read from. This can be a regex that enables you to read from multiple streams and handle the messages in the same way. It is to be kept in mind that the messages from different streams will be passed to the same mapper-function.
        - oldest-processed-messages-in-s - The oldest message which will be processed by stream in second. By default the value is 604800 (1 week)
        - changelog-topic-replication-factor - the internal changelog topic replication factor. By default the value is 3
        - channels - Configure rabbitmq channels for increasing parallelism to more than the number of partitions.
            - worker-count - Each channel name can have multiple workers. This defines how many messages you want to process parallely.
            - retry - This defines the channel retries. Used specifically for retrying messages processed from channels.
                - type - defines the type of retry (linear,exponential)
                - count - number of retries before message is sent to channel DLQ.
                - enabled - if channel retries are enabled or not.
        - producer - Configuration for KafkaProducer. All properties supported by [Kafka Producer Config](https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html) can be provided as kebab case keywords
            - bootstrap.servers - A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
            - acks - The number of acknowledgments the producer requires the leader to have received before considering a request complete. Valid values are [all, -1, 0, 1].
            - retries - Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.
            - key.serializer - Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface.
            - value.serializer - Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface.
            - max.in.flight.requests.per.connection - The maximum number of unacknowledged requests the client will send on a single connection before blocking.
            - enable.idempotence - When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer retries due to broker failures, etc., may write duplicates of the retried message in the stream.
- batch-routes - This has been explained in the [Batch Routes](https://github.com/gojek/ziggurat/tree/master#batch-consumption-using-kafka-consumer-api) section above. All the properties provided with [Kafka Consumer Config](https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html) are accepted as kebab case keywords
- ssl - All Kafka [SSL configs](https://kafka.apache.org/28/javadoc/org/apache/kafka/common/config/SslConfigs.html) and [SASL configs](https://kafka.apache.org/28/javadoc/org/apache/kafka/common/config/SaslConfigs.html) can be provided as kebab case keywords. These configs are automatically applied to all kafka stream, kafka producer and kafka consumer objects created in Ziggurat.
- statsd - Formerly known as datadog, The statsd host and port that metrics should be sent to.
- sentry - Whenever a :failure keyword is returned from the mapper-function or an exception is raised while executing the mapper-function, an event is sent to sentry. You can skip this flow by disabling it.
- rabbit-mq-connection - The details required to make a connection to rabbitmq. We use rabbitmq for the retry mechanism.
- rabbit-mq - The queues that are part of the retry mechanism
- retry - The number of times the message should be retried and if retry flow should be enabled or not. If retry is disabled, and :retry is returned from mapper function, messages will be lost.
- jobs - The number of consumers that should be reading from the retry queues and the prefetch count of each consumer
- http-server - Ziggurat starts an http server by default and gives apis for ping health-check and deadset management. This defines the port and the number of threads of the http server. It also controls the graceful shutdown timeout of the HTTP server. Default is `30000ms`
- new-relic - If report-errors is true, whenever a :failure keyword is returned from the mapper-function or an exception is raised while executing it, an error is reported to new-relic. You can skip this flow by disabling it.
