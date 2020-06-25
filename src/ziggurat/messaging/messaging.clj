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



(defstate connection
          :start (do (log/info "Initializing Messaging")
                     (initialise-messaging-library)
                     (messaging-interface/start-connection @messaging-impl ziggurat.config/config (:stream-routes (mount/args))))
          :stop (messaging-interface/stop-connection @messaging-impl connection ziggurat.config/config (:stream-routes (mount/args))))

(defn start-connection [config stream-routes]
  (do (initialise-messaging-library)
      (log/info "Initializing Messaging Library")
      (messaging-interface/start-connection @messaging-impl config stream-routes)))

