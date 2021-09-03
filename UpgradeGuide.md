## Upgrade Guide for Ziggurat version 4.2.0

Ziggurat v4.20 introduces structured-logging using cambium. Ziggurat comes pre-packaged with cambium and newly scaffolded actors come pre-configured with JSON structured logging. To enable srtuctured logging in your existing actors, the following steps are required

### Steps to enable structured logging

Replace log4j2.xml with the attached logback.xml file (Only for existing actors. New actors come pre-bundled with logback.xml file).

Adjust the log levels as per your convenience.

Sample `logback.xml`

```xml
<configuration name="ziggurat-config">
    <property name="type" value="${ZIGGURAT_LOG_FORMAT:-text}"/>

    <if condition='property("type").equals("json")'>
        <then>
            <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
                <encoder class="ch.qos.logback.core.encoder.LayoutWrappingEncoder">
                    <layout class="cambium.logback.json.FlatJsonLayout">
                        <jsonFormatter class="ch.qos.logback.contrib.jackson.JacksonJsonFormatter">
                            <prettyPrint>false</prettyPrint>
                        </jsonFormatter>
                        <timestampFormat>yyyy-MM-dd'T'HH:mm:ss.SSS'Z'</timestampFormat>
                        <appendLineSeparator>true</appendLineSeparator>
                    </layout>
                </encoder>
            </appender>
        </then>
    </if>

    <if condition='property("type").equals("text")'>
        <then>
            <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
                <layout class="ch.qos.logback.classic.PatternLayout">
                    <pattern>
                        [%-5level] %d [%t] %c:%M: %m { %mdc }%n
                    </pattern>
                </layout>
            </appender>
        </then>
    </if>

    <logger name="ziggurat-config" level="debug" additivity="false">
        <appender-ref ref="STDOUT"/>
    </logger>
    <root level="info">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

Include the following logback dependencies in your project.clj and remove the log4j2 dependencies if there are any

```clojure
[ch.qos.logback/logback-classic "1.2.3"]
[ch.qos.logback.contrib/logback-json-classic "0.1.5"]
[ch.qos.logback.contrib/logback-jackson "0.1.5"]
[net.logstash.logback/logstash-logback-encoder "6.6"]
```

Using the kernel config management on kernel.gojek.dev, set a new key ZIGGURAT_LOG_FORMAT with value "json" .

We are using cambium to accomplish structured logging. 

Prerequisite: Include [cambium.core  :as clog] in the namespace where structured logging is needed. 

Ziggurat comes pre-packaged with cambium.

Users have two ways to make use of structured logging. 

Via the logging macros of cambium

(clog/info {:cambium_key1 "value1" :cambium_key2 "value2"} "Cambium log 1")

The first parameter has to be a clojure map, while the second parameter has to be a string.

Sample output

```json
{
  "timestamp" : "2021-08-25T09:50:00.832Z",
  "level" : "INFO",
  "thread" : "main",
  "cambium_key1" : "value1",
  "ns" : "dss-transport.main",
  "line" : 59,
  "cambium_key2" : "value2",
  "column" : 19,
  "logger" : "dss-transport.main",
  "message" : "Cambium log 1",
  "context" : "default"
}
```

Via the context macro

```clojure
(clog/with-logging-context {:cambium_key1 "value1" :cambium_key2 "value2" 
                            :nested_key { :key1 {:key2 "value3"}}}
                                             (log/info "Cambium log with context")
                                             (log/info "Cambium log 2 with context"))
```

Users just need to wrap their existing logs around a logging-context and all the logs within this will have the map supplied along with the context as additional parameters. These logs can be generated either via clojure.logging.tools or via cambium



## Upgrade Guide for Ziggurat 3.1.0

### Configuration Changes
There were some breaking changes to kafka streams library being used by Ziggurat version 3.0. 
Ziggurat 3.0 has upgraded kafka streams to v2.1. This requires the user to follow certain steps while
upgrading. These are explained below.


For upgrading Ziggurat to 3.1.0, per [Apache Kafka Upgrade Guide](https://kafka.apache.org/21/documentation/streams/upgrade-guide) 
and [KIP-268](https://cwiki.apache.org/confluence/display/KAFKA/KIP-268%3A+Simplify+Kafka+Streams+Rebalance+Metadata+Upgrade#KIP-268:SimplifyKafkaStreamsRebalanceMetadataUpgrade-Upgradingto2.0:), the user need to follow these steps
- Add a new config `upgrade-from` for each of the top level config-map in :stream-router section of 
  config.edn. This config can be added either in `config.edn` file in the project or as an environment 
  variable,
  as explained below.
- Do a rolling deploy of the application with the newly added configuration (above).
- Remove the configs added above
- Do a rolling deploy of the application again.


This can be understood with the help of an example. For the following stream-router configuration
```
:stream-router        {:topic-entity-1       {:application-id                 "application-1"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "first-topic"}}
                      {:topic-entity-2       {:application-id                 "application-2"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "second-topic"}}
```

For projects using Ziggurat version <= 2.7.2, (`[tech.gojek/ziggurat "2.6.0"]`, for example), 
the new stream-router config should look like this

```
:stream-router        {:topic-entity-1       {:application-id                 "application-1"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "first-topic"
                                              :upgrade-from                   "0.11.0"}}
                      {:topic-entity-2       {:application-id                 "application-2"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "second-topic"
                                              :upgrade-from                   "0.11.0"}}
```

Or, if the user is not comfortable with modifying `config.edn`, equivalent environment variables can be added to
the project environment.
```
ZIGGURAT_STREAM_ROUTER_TOPIC_ENTITY_1_UPGRADE_FROM=0.11.0
ZIGGURAT_STREAM_ROUTER_TOPIC_ENTITY_2_UPGRADE_FROM=0.11.0
```

For projects using Ziggurat version > 2.7.2, (`[tech.gojek/ziggurat "2.12.1"]`, for example), the new stream-router config 
should look like

```
:stream-router        {:topic-entity-1       {:application-id                 "application-1"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "first-topic"
                                              :upgrade-from                   "1.1"}}
                      {:topic-entity-2       {:application-id                 "application-2"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "second-topic"
                                              :upgrade-from                   "1.1"}}
```
Or, an equivalent environment variable can be added for each of the entiteis in `stream-router` section. Like this
```
ZIGGURAT_STREAM_ROUTER_TOPIC_ENTITY_1_UPGRADE_FROM=1.1
ZIGGURAT_STREAM_ROUTER_TOPIC_ENTITY_2_UPGRADE_FROM=1.1
```

### Using Middlewares

In the versions preceding 3.0, Ziggurat would only process messages which were serialized
in protobuf format. This was a major limitation as users could not use any other formats like JSON 
or Avro. 

In Ziggurat 3.0, the logic for deserialization has been extracted out as middlewares 
which can be used not only for deserializing a message in any given format, but
can be plugged together to perform a set of tasks before a message is processed.

You can read more about them at [Ziggurat Middlewares](https://github.com/gojek/ziggurat#middleware-in-ziggurat)

As far as message processing is concerned, messages will be provided as byte arrays and the user
has to explicitly use `ziggurat.middleware.default/protobuf->hash` to deserialize a message
before processing it.

For example, in previous versions, for this stream-routes configuration,
```
:stream-router        {:topic-entity-1       {:application-id                 "application-1"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "first-topic"}}
                                              :channels {:geofence-channel {:worker-count [10 :int]
                                                         :retry            {:count [3 :int]}}}}
                      {:topic-entity-2       {:application-id                 "application-2"
                                              :bootstrap-servers              "localhost:9092"
                                              :origin-topic                   "second-topic"}}
                                                                                                                                                                             :enabled [true :bool]
```

a mapper function in a Ziggurat-based project would look like this
```
(init/main start-fn stop-fn {:topic-entity-1 {:handler-function  mapper-fn 
                                              :geofence-channel  channel-mapper-fn}
                             topic-entity-2  {:handler-function  mapper-fn-2}})
```

An upgrade to Ziggurat 3.0 would require the user to write a new mapper function 
which explicitly deserializes the message using the proto middleware (provided in Ziggurat be default) 
before passing it as an argument to the old mapper function.

```
(def middleware-based-mapper-fn
  (-> mapper-fn
      (ziggurat.middleware.default/protobuf->hash com.gojek.esb.booking.BookingLogMessage :topic-entity-1)))
      
(def middleware-based-mapper-fn-2
  (-> mapper-fn-2
      (ziggurat.middleware.default/protobuf->hash com.gojek.esb.booking.BookingLogMessage :topic-entity-2)))
      
(def middleware-based-channel-mapper-fn
  (-> channel-mapper-fn
      (ziggurat.middleware.default/protobuf->hash com.gojek.esb.booking.BookingLogMessage :topic-entity-1)))

(init/main start-fn stop-fn {:topic-entity-1 {:handler-function  middleware-based-mapper-fn 
                                              :geofence-channel  middleware-based-channel-mapper-fn}
                             topic-entity-2  {:handler-function  middleware-based-mapper-fn-2}})
```
A similar change will be required for all the handler-functions/channel-functions in 
`stream-routes` map which is passed to `init/main`.

Development is under way to provide a JSON middleware for deserializing JSON messages. 
It is expected to be available in 3.1.0-alpha.2.

