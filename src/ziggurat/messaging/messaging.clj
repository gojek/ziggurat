(ns ziggurat.messaging.messaging
  (:require [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]))

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