(ns ziggurat.messaging.connection
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq]
            [mount.core :as mount :refer [defstate start]]
            [sentry.core :as sentry]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.channel :refer [get-keys-for-topic]])
  (:import [com.rabbitmq.client ShutdownListener]
           [java.util.concurrent Executors]))

(defn- is-connection-required? []
  (let [stream-routes (:stream-routes (mount/args))
        all-channels  (reduce (fn [all-channel-vec [topic-entity _]]
                                (concat all-channel-vec (get-keys-for-topic stream-routes topic-entity)))
                              []
                              stream-routes)]
    (or (pos? (count all-channels))
        (-> (ziggurat-config) :retry :enabled))))

(defn- channel-threads
  [channels]
  (reduce (fn [sum [_ channel-config]]
            (+ sum (:worker-count channel-config))) 0 channels))

(defn- total-thread-count
  []
  (let [stream-routes (:stream-router (ziggurat-config))
        worker-count  (get-in (ziggurat-config) [:jobs :instant :worker-count])]
    (reduce (fn [sum [_ route-config]]
              (+ sum (channel-threads (:channels route-config)) worker-count)) 0 stream-routes)))

(defn- start-connection []
  (log/info "Connecting to RabbitMQ")
  (when (is-connection-required?)
    (try
      (let [connection (rmq/connect (assoc (:rabbit-mq-connection (ziggurat-config)) :executor (Executors/newFixedThreadPool (total-thread-count))))]
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
  (when (is-connection-required?)
    (rmq/close conn)
    (log/info "Disconnected from RabbitMQ")))

(defstate connection
  :start (start-connection)
  :stop (stop-connection connection))
