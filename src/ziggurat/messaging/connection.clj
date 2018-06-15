(ns ziggurat.messaging.connection
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq]
            [mount.core :refer [defstate]]
            [sentry.core :as sentry]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]])
  (:import [com.rabbitmq.client ShutdownListener]))


(defn- start-connection []
  (log/info "Connecting to RabbitMQ")
  (when (-> (ziggurat-config) :retry :enabled)
    (try
      (let [connection (rmq/connect (:rabbit-mq-connection (ziggurat-config)))]
        (doto connection
          (.addShutdownListener
            (reify ShutdownListener
              (shutdownCompleted [_ cause]
                (when-not (.isInitiatedByApplication cause)
                  (log/error cause "RabbitMQ connection shut down due to error")))))))
      (catch Exception e
        (sentry/report-error sentry-reporter e "Error while starting RabbitMQ connection")
        (throw e)))))

(defn- stop-connection [conn]
  (when conn
    (rmq/close conn)
    (log/info "Disconnected from RabbitMQ")))

(defstate connection
  :start (start-connection)
  :stop (stop-connection connection))
