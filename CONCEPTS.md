## Concepts

Stream Routes
 -
Stream routes are analogous to HTTP routes in the sense that instead of an API call, messages from a kafka stream are routed to a handler function .
In Ziggurat we use Kafka streams to get messages from the particular topic (or a set of topics). Ziggurat supports reading
data from multiple streams and since each stream can possibly route messages to a different handler-function the concept
of stream routes acts as a really significant abstraction in the framework.

Retrying Messages
-


Actor Routes
 -
Ziggurat starts a HTTP server to provide APIs for ping health check and viewing, retrying messages in the deadset queues.
It also enables the user to pass in their own routes so that they don't have to set up their own server. The [Readme](README.md)
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
Ziggurat is a stream processing framework, and in a lot of our use cases it is necessary for us to keep a minimum consumer lag.
But we faced issues where the mapper-function execution time was too high and we could not increase the number of parallel consumers
due to the constraint set by the number of partitions. So we introduced the concept of channels. A channel sets up an exchange
and queues in rabbitmq and pushes messages directly to rabbitmq from kafka (without applying the mapper-function). Consumers
then read the messages from the rabbitmq queues and since we can scale up the consumers on rabbitmq indefinitely we can keep
the consumer lag in check.
Let us understand this with an example. Assume that we a topic, flash, that produces message at 10 messages/sec and has 2 partitions.
To keep the consumer lag from getting out of hand our mapper-function can take a maximum processing time of 200 ms
(2 consumers, so every consumer has to read 5 messages per second). But due to an external api call the execution time goes up
to 500 ms. Thus we can consume only 4 messages/sec now. To fix this the consumers can read messages from kafka and directly put
them into rabbitmq. Then we can have 5 or 6 consumers on rabbitmq that read the messages and process them. That is all that
channels do.
