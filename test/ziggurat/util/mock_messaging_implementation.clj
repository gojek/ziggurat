(ns ziggurat.util.mock-messaging-implementation
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.messaging-interface :refer [MessagingProtocol]]))

(defn start-connection [config stream-routes]  nil)

(defn stop-connection [config stream-routes] nil)

(defn publish
  ([exchange message-payload]
   (publish exchange message-payload nil))
  ([exchange message-payload expiration] nil))

(defn create-and-bind-queue
  ([queue-name exchange-name]
   (create-and-bind-queue queue-name exchange-name nil))
  ([queue-name exchange-name dead-letter-exchange] nil))

(defn get-messages-from-queue
  ([queue-name ack?] (get-messages-from-queue queue-name ack? 1))
  ([queue-name ack? count] nil))

(defn process-messages-from-queue [queue-name count processing-fn] nil)

(defn start-subscriber [prefetch-count wrapped-mapper-fn queue-name] nil)

(defn consume-message [ch meta payload ack?] nil)

(deftype MockMessaging []
  MessagingProtocol
  (start-connection [impl config stream-routes] (start-connection config stream-routes))
  (stop-connection [impl config stream-routes] (stop-connection config stream-routes))
  (create-and-bind-queue [impl queue-name exchange-name]
    (create-and-bind-queue queue-name exchange-name))
  (create-and-bind-queue [impl queue-name exchange-name dead-letter-exchange]
    (create-and-bind-queue queue-name exchange-name dead-letter-exchange))
  (publish [impl exchange message-payload] (publish exchange message-payload))
  (publish [impl exchange message-payload expiration] (publish exchange message-payload expiration))
  (get-messages-from-queue [impl queue-name ack?] (get-messages-from-queue queue-name ack?))
  (get-messages-from-queue [impl queue-name ack? count] (get-messages-from-queue queue-name ack? count))
  (process-messages-from-queue [impl queue-name count processing-fn] (process-messages-from-queue queue-name count processing-fn))
  (start-subscriber [impl prefetch-count wrapped-mapper-fn queue-name] (start-subscriber prefetch-count wrapped-mapper-fn queue-name))
  (consume-message [impl ch meta payload ack?] (consume-message ch meta payload ack?)))

