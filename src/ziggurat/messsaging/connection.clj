(ns ziggurat.messsaging.connection
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [langohr.core :as rmq]
            [mount.core :refer [defstate]]
            [sentry.core :as sentry])
  (:import [com.rabbitmq.client AlreadyClosedException ShutdownListener]
           (java.util.concurrent ExecutorService TimeUnit)))


(defn- start-connection []
  (log/info "Connecting to RabbitMQ")
  (try
    (let [connection (rmq/connect (:rabbit-mq-connection config))]
      (doto connection
        (.addShutdownListener
          (reify ShutdownListener
            (shutdownCompleted [_ cause]
              (when-not (.isInitiatedByApplication cause)
                (log/error cause "RabbitMQ connection shut down due to error")))))))
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while starting RabbitMQ connection")
      (throw e))))

(defn- stop-connection [conn]
  (rmq/close conn)
  (log/info "Disconnected from RabbitMQ"))

(defstate connection
  :start (start-connection)
  :stop (stop-connection connection))
