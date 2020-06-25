(ns ziggurat.messaging.messaging-interface)

(defprotocol MessagingProtocol
  "A protocol that defines the interface for queue-based retry implementation libraries. Any type or class that implements this protocol can be passed to the messaging namespace to be used for retrying messages.
 Example implementations for this protocol:

 start-connection [config stream-routes]
 This is used to initialize the messaging library so that it push messages to the retry backend.
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - config: It is the config map defined in the `config.edn` file. It includes the entire config map
    - stream-routes: It is a map containing topic entities and their handler functions, channels and their handler functions

 stop-connection [impl connection config stream-routes]
 This is used to stop the messaging library and cleanup resources.
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - connection: The connection object that is used which can be used to close and cleanup network connections to the retry backend
    - config: It is the config map defined in the `config.edn` file. It includes the entire config map
    - stream-routes: It is a map containing topic entities and their handler functions, channels and their handler functions

 create-and-bind-queue [impl queue-name exchange-name dead-letter-exchange]
 This is used to create a queue and bind it to an exchange.
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - queue-name: The name of the queue which will be bound the exchange
    - exchange-name: The name of the exchange to bind the queue to, Example if `test_application_queue` is bound to `test_application_exchange` then all messages which need to be sent to `test_application_queue`
    will have to be sent to the `test_application_exchange` which will route it to the bound queue.
    - dead-letter-exchange: The name of the dead-letter-exchange if specified will be bound the queue which will in turn route all messages which have exceeded the max retries to the specified dead-letter-exchange

 get-messages-from-queue [impl queue-name ack? count]
 This is used to fetch and deserialize them messages from the queue.
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - queue-name: this is the name of the queue from where to fetch messages
    - ack?: a boolean flag which indicates if the message will be acknowledged upon consumption, ack-ing the message should discard it from the queue preventing a re-consumption.
    - count: `count` number of messages will be fetched from the queue. If not provided it will default to an arbitrary value, ideally `1`.

 process-messages-from-queue [impl queue-name count processing-fn]
 This is used to process messages from the queue. This method does not return the messages unlike `get-messages-from-queue`, instead it runs the `processing-fn` against every message consumed from the queue
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - queue-name: the name of the queue to consume messages from
    - ack?: a boolean flag which indicates if the message will be acknowledged upon consumption, ack-ing the message should discard it from the queue preventing a re-consumption.
    - count: `count` number of messages will be consumed from the queue. If not provided it will default to an arbitrary value, ideally `1`.

 start-subscriber [impl prefetch-count wrapped-mapper-fn queue-name]
 This method is used to start subscribers for a specific queue
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - prefetch-count: The maximum number of unacknowledged messages that a consumer can receive at once
    - wrapped-mapper-fn: This is the processing-fn which will run against every messaged consumed from the queue
    - queue-name: This is the name of the queue to which a subscriber will subscribe to

 consume-message [impl ch meta payload ack?]
 ; Todo should be removed as it is only being used for testing
 This method deserializes the message consumed from the queue
 Args:
    - impl: the class object that implements this protocol, i.e. an instance of the deftype for example
    - ch: A Channel object
    - meta: metadata containing information about the message payload
    - payload: the message payload in byte-array format as fetched from the queue
    - ack?: a boolean flag which indicates if the message will be acknowledged upon consumption, ack-ing the message should discard it from the queue preventing a re-consumption.
 "

  (start-connection [impl config stream-routes])
  (stop-connection [impl config stream-routes])
  (publish
    [impl exchange message-payload]
    [impl exchange message-payload expiration])
  (create-and-bind-queue
    [impl queue-name exchange-name]
    [impl queue-name exchange-name dead-letter-exchange])
  (get-messages-from-queue
    [impl queue-name ack?]
    [impl queue-name ack? count])
  (process-messages-from-queue [impl queue-name count processing-fn])
  (start-subscriber [impl prefetch-count wrapped-mapper-fn queue-name])
  (consume-message [impl ch meta payload ack?]))
