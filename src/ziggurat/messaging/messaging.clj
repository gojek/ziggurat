(ns ziggurat.messaging.messaging
  (:require [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.util :as util]
            [ziggurat.messaging.messaging-interface :as messaging-interface]
            [clojure.tools.logging :as log]))

(def messaging-impl (atom nil))

(defn get-implementation []
  (if (nil? @messaging-impl)
    (throw (Exception. "Messaging Library has not been initialized, please make sure the messaging library has been initialized"))
    @messaging-impl))

(defn- get-messaging-implementor-constructor []
  (if-let [configured-metrics-class-constructor (get-in (ziggurat-config) [:messaging :constructor])]
    (let [configured-constructor-symbol (symbol configured-metrics-class-constructor)
          constructor-namespace         (namespace configured-constructor-symbol)
          _                             (require [(symbol constructor-namespace)])
          metric-constructor            (resolve configured-constructor-symbol)]
      (if (nil? metric-constructor)
        (throw (ex-info "Incorrect messaging_interface implementation constructor configured. Please fix it." {:constructor-configured configured-constructor-symbol}))
        metric-constructor))
    rmqw/->RabbitMQMessaging))

(defn initialise-messaging-library []
  (let [messaging-impl-constructor (get-messaging-implementor-constructor)]
    (reset! messaging-impl (messaging-impl-constructor))))

(defn start-connection [config stream-routes]
  (initialise-messaging-library)
  (when (util/is-connection-required? (:ziggurat config) stream-routes)
    (log/info "Initialized Messaging Library")
    (messaging-interface/start-connection (get-implementation) config stream-routes)))

(defn stop-connection [config stream-routes]
  (when-not (nil? @messaging-impl)
    (log/info "Stopping Messaging Library")
    (messaging-interface/stop-connection (get-implementation) config stream-routes)))

(defn create-and-bind-queue
  ([queue-name exchange-name]
   (messaging-interface/create-and-bind-queue (get-implementation) queue-name exchange-name))
  ([queue-name exchange-name dead-letter-exchange]
   (messaging-interface/create-and-bind-queue (get-implementation) queue-name exchange-name dead-letter-exchange)))

(defn publish
  ([exchange message-payload]
   (messaging-interface/publish (get-implementation) exchange message-payload))
  ([exchange message-payload expiration]
   (messaging-interface/publish (get-implementation) exchange message-payload expiration)))

(defn get-messages-from-queue
  ([queue-name ack?]
   (messaging-interface/get-messages-from-queue (get-implementation) queue-name ack?))
  ([queue-name ack? count]
   (messaging-interface/get-messages-from-queue (get-implementation) queue-name ack? count)))

(defn process-messages-from-queue [queue-name count processing-fn]
  (messaging-interface/process-messages-from-queue (get-implementation) queue-name count processing-fn))

(defn start-subscriber [prefetch-count wrapped-mapper-fn queue-name]
  (messaging-interface/start-subscriber (get-implementation) prefetch-count wrapped-mapper-fn queue-name))

(defn consume-message [ch meta payload ack?]
  (messaging-interface/consume-message (get-implementation) ch meta payload ack?))

