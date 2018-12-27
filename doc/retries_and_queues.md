### Retries, Queues, and How to Use them

#### Content
1. [Introduction](#introduction)
2. [Topology of the Queues](#topology-of-the-queues)
3. [Queue Stats](#queue-stats)
4. [Peaking & Replaying DeadSet](#peaking-and-replaying-deadset)
5. [What happens when the actor goes down](#what-happens-when-the-actor-goes-down)

#### Introduction

When processing a stream of events, there are many times where the processing might fail.
Either because of the issue in the function logic or some issue downstream.

In the first case, we don't want to lose the message. In second, we
possibly want to retry the message and expect that the downstream
service might be down intermittently.

This acts as a foundation of the requirement of a queue to fall back to.

Ziggurat provides Retry-As-A-Service, which means your messages
automatically get queued on failures.

Let's take a deep dive

#### Topology of Queues

Every application using ziggurat creates 3 dedicated queues in Rabbitmq:

1. **Delayed Queue:**
When an error occurs for a message in the mapper-function. The message is put in the delay queue with a TTL (can be set in the config).
They wait in the queue until the TTL expires. Then they are put into the instant queue.

2. **Instant Queue:**
The retry logic in ziggurat reads messages from the instant queue and retries them.
If the retry fails again it puts the message back into the delay queue with the predefined TTL.
This happens 3(or however many times the RETRY_COUNT config defines it). If the message still does not succeed it is put into the Dead-set queue.

**This results in a linear backoff for the retires of the same message.**

3. **Dead Set Queue:**
Messages are put into the dead set when they fail to successfully process even after however many times the RETRY_COUNT defines it.
You can retry messages from the dead set queue by making an API call to the actor.

#### Peaking and Replaying DeadSet

DeadSet is the last place a message can reach in its life cycle because of failures.
Ziggurat does not automatically retry the messages in the dead set as it could be due to a bug in the mapper-function and the
user can trigger the retry of messages once the problem has been fixed. 

There are two built-in APIs to view and trigger retry on the DeadSet:

  Assuming your stream routes are defined as follows:
  ```clojure
    (ziggurat/main start-fn stop-fn {:stream-id-1 {:handler-fn main-fn}
                                     :stream-id-2 {:handler-fn (fn [] :outbound-channel)
                                                   :outbound-channel main-fn}})
```

1. Peek: A GET API where you can fetch `N` messages from the deadset to see which message went in.

  - For multi-stream applications (that read from multiple kafka streams)
  ```
  curl -X GET \
    'http://localhost:8010/v1/dead_set?count=10&topic-entity=stream-id-1'
  ```
  - For multi-stream applications with channels
  ```
  curl -X GET \
    'http://localhost:8010/v1/dead_set?count=10&topic-entity=stream-id-2&channel=outbound-channel'
  ```
  where
   - `10` is the number of messages you wish to view
   - `8010` is the port that is listening to the HTTP requests
   - `stream-id-*` is the topic entity of your actor
   - `outbound-channel` is the channel on topic-entity `stream-id-2`

2. Replay: A POST API where you can select `N` messages to be retried from the DeadSet.

  - For multi-stream applications (that read from multiple kafka streams)
  ```
  curl -X POST \
    http://localhost:8010/v1/dead_set/replay \
    -H 'content-type: application/json' \
    -d '{"count":"10", "topic_entity":"stream-id-1"}'
  ```
  - For multi-stream applications with channels
  ```
  curl -X POST \
    http://localhost:8010/v1/dead_set/replay \
    -H 'content-type: application/json' \
    -d '{"count":"10", "topic_entity":"stream-id-2", "channel": "outbound-channel"}'
  ```
  where
   - `10` is the number of messages you wish to retry
   - `8010` is the port that is listening to the HTTP requests
   - `stream-id-*` is the topic entity of your actor
   - `outbound-channel` is the channel on topic-entity `stream-id-2`

**You cannot get/replay a specific message**

#### What happens when the actor goes down

When the actor goes down all the processing stops. So there won't be any new messages getting read from Kafka.
No new messages will come into any of the queues.

