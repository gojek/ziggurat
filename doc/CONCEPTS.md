## Concepts

Stream Routes
 -
Stream routes are analogous to HTTP routes in the sense that instead of an API call, messages from a Kafka stream are routed to a handler function.
In Ziggurat, we use Kafka streams to get messages from the particular topic (or a set of topics). Ziggurat supports reading
data from multiple streams and since each stream can possibly route messages to a different handler-function the concept
of stream routes act as a really significant abstraction in the framework.

Retrying Messages
-
Please refer the [Retries and Queues](retries_and_queues.md) document for this.

Actor Routes
 -
Ziggurat starts an HTTP server to provide APIs for ping health check and viewing, retrying messages in the deadset queues.
It also enables the user to pass in their own routes so that they don't have to set up their own server. The [Readme](../README.md)
has details on how to pass in the actor routes.

Channels
 -
The maximum number of threads that we can run for reading from kafka is the number of partitions that the topic has. Further details are explained
[here](https://docs.confluent.io/current/streams/architecture.html). This is an excerpt from the document
```
Slightly simplified, the maximum parallelism at which your application may run is bounded by the maximum number of stream
tasks, which itself is determined by maximum number of partitions of the input topic(s) the application is reading from.
For example, if your input topic has 5 partitions, then you can run up to 5 applications instances. These instances will
collaboratively process the topic’s data. If you run a larger number of app instances than partitions of the input topic,
the “excess” app instances will launch but remain idle; however, if one of the busy instances goes down, one of the idle
; instances will resume the former’s work. We provide a more detailed explanation and example in the FAQ.
```
Ziggurat is a stream processing framework, and in a lot of our use cases, it is necessary for us to keep a minimum consumer lag.
But we faced issues where the mapper-function execution time was too high and we could not increase the number of parallel consumers
due to the constraint set by the number of partitions. So we introduced the concept of channels. A channel sets up an exchange
and queues in Rabbitmq and pushes messages directly to Rabbitmq from Kafka (without applying the mapper-function). Consumers
then read the messages from the Rabbitmq queues and since we can scale up the consumers on Rabbitmq indefinitely we can keep
the consumer lag in check.
Let us understand this with an example. Assume that we consume from a topic, flash, that produces message at 10 messages/sec and has 2 partitions.
To keep the consumer lag from getting out of hand our mapper-function can take a maximum processing time of 200 ms
(2 consumers, so every consumer has to read 5 messages per second). But due to an external API call the execution time goes up
to 500 ms. Thus we can consume only 4 messages/sec now. To fix this the consumers can read messages from Kafka and directly put
them into Rabbitmq. Then we can have 5 or 6 consumers on Rabbitmq that read the messages and process them. That is all that
channels do. 

## Http Server
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


## Toggle streams in running actor

Feature implementation of [issue #56](https://github.com/gojek/ziggurat/issues/56). Stop and start streams on a running process using nREPL. A nREPL server starts at `port 7011`(default) when an actor using ziggurat starts. Check `ZIGGURAT_NREPL_SERVER_PORT` in your config.

Connect to the shell using

```shell
lein repl :connect <host>:<port>
```

The functions can be accessed via the following commands to stop and start streams using their `topic-entity`

```shell
> (ziggurat.streams/stop-stream :booking)
> (ziggurat.streams/start-stream :booking)
```

where `booking` is the `topic-entity`


### Stop all streams
```shell
(mount.core/stop #'ziggurat.streams/stream) #run this on all pods/VMs
```
