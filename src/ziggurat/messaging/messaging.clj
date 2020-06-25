(ns ziggurat.messaging.messaging
  (:require [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.messaging-interface :as messaging-interface]
            [mount.core :as mount :refer [defstate]]
            [mount.core :as mount]
            [clojure.tools.logging :as log]))

(declare connection)

(def messaging-impl (atom nil))

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
  (do (initialise-messaging-library)
      (log/info "Initializing Messaging Library")
      (messaging-interface/start-connection @messaging-impl config stream-routes)))

(defn stop-connection [config stream-routes]
  (do (log/info "Stopping Messaging Library")
      (messaging-interface/stop-connection @messaging-impl config stream-routes)))

(defn publish
  ([exchange message-payload]
   (messaging-interface/publish @messaging-impl exchange message-payload))
  ([exchange message-payload expiration]
   (messaging-interface/publish @messaging-impl exchange message-payload expiration)))

(defn get-messages-from-queue
  ([queue-name ack?]
   (messaging-interface/get-messages-from-queue @messaging-impl queue-name ack?))
  ([queue-name ack? count]
   (messaging-interface/get-messages-from-queue @messaging-impl queue-name ack? count)))

(defn process-messages-from-queue [queue-name count processing-fn]
  (messaging-interface/process-messages-from-queue @messaging-impl queue-name count processing-fn))

(defn start-subscriber [prefetch-count wrapped-mapper-fn queue-name]
  (messaging-interface/start-subscriber @messaging-impl prefetch-count wrapped-mapper-fn queue-name))

(defn consume-message [ch meta payload ack?]
  (messaging-interface/consume-message @messaging-impl ch meta payload ack?))

