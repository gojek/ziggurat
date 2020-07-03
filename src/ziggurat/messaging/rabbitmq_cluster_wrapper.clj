(ns ziggurat.messaging.rabbitmq-cluster-wrapper
  (:require [ziggurat.messaging.rabbitmq.cluster.connection :as rmq-cluster-conn]
            [ziggurat.messaging.rabbitmq.cluster.producer :as rmqc-producer]
            [ziggurat.messaging.messaging-interface :refer [MessagingProtocol]]
            [ziggurat.messaging.rabbitmq.consumer :as rmq-consumer]
            [ziggurat.messaging.rabbitmq.producer :as rmq-producer]))

(def connection (atom nil))

(def rabbitmq-cluster-config (atom nil))

(defn get-connection [] @connection)

(defn start-connection [config stream-routes]
  (when (nil? (get-connection))
    (reset! rabbitmq-cluster-config (get-in config [:ziggurat :rabbit-mq-connection]))
    (reset! connection (rmq-cluster-conn/start-connection config))))

(defn stop-connection [config stream-routes]
  (when-not (nil? (get-connection))
    (reset! rabbitmq-cluster-config nil)
    (rmq-cluster-conn/stop-connection (get-connection) config)
    (reset! connection nil)))

(defn publish
  ([exchange message-payload]
   (publish exchange message-payload nil))
  ([exchange message-payload expiration]
   (rmq-producer/publish (get-connection) exchange message-payload expiration)))

(defn create-and-bind-queue
  ([queue-name exchange-name]
   (create-and-bind-queue queue-name exchange-name nil))
  ([queue-name exchange-name dead-letter-exchange]
   (rmqc-producer/create-and-bind-queue @rabbitmq-cluster-config (get-connection) queue-name exchange-name dead-letter-exchange)))

(defn get-messages-from-queue
  ([queue-name ack?]
   (get-messages-from-queue queue-name ack? 1))
  ([queue-name ack? count]
   (rmq-consumer/get-messages-from-queue (get-connection) queue-name ack? count)))

(defn process-messages-from-queue [queue-name count processing-fn]
  (rmq-consumer/process-messages-from-queue (get-connection) queue-name count processing-fn))

(defn start-subscriber [prefetch-count wrapped-mapper-fn queue-name]
  (rmq-consumer/start-subscriber (get-connection) prefetch-count wrapped-mapper-fn queue-name))

(defn consume-message [ch meta payload ack?]
  (rmq-consumer/consume-message ch meta payload ack?))

(deftype RabbitMQMessaging [] MessagingProtocol
         (start-connection [impl config stream-routes]
           (start-connection config stream-routes))
         (stop-connection [impl config stream-routes]
           (stop-connection config stream-routes))
         (create-and-bind-queue [impl queue-name exchange-name]
           (create-and-bind-queue queue-name exchange-name))
         (create-and-bind-queue [impl queue-name exchange-name dead-letter-exchange]
           (create-and-bind-queue queue-name exchange-name dead-letter-exchange))
         (publish [impl exchange message-payload]
           (publish exchange message-payload))
         (publish [impl exchange message-payload expiration]
           (publish exchange message-payload expiration))
         (get-messages-from-queue [impl queue-name ack?]
           (get-messages-from-queue queue-name ack?))
         (get-messages-from-queue [impl queue-name ack? count]
           (get-messages-from-queue queue-name ack? count))
         (process-messages-from-queue [impl queue-name count processing-fn]
           (process-messages-from-queue queue-name count processing-fn))
         (start-subscriber [impl prefetch-count wrapped-mapper-fn queue-name]
           (start-subscriber prefetch-count wrapped-mapper-fn queue-name))
         (consume-message [impl ch meta payload ack?]
           (consume-message ch meta payload ack?)))
