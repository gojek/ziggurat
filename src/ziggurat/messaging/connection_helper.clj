(ns ziggurat.messaging.connection-helper
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq]
            [mount.core :as mount :refer [defstate start]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.channel :refer [get-keys-for-topic]]
            [ziggurat.messaging.util :as util]
            [ziggurat.util.error :refer [report-error]])
  (:import [com.rabbitmq.client ShutdownListener ConnectionFactory AddressResolver]
           [java.util.concurrent Executors ExecutorService]
           [com.rabbitmq.client.impl DefaultCredentialsProvider]))

(defn is-connection-required? []
  (let [stream-routes (:stream-routes (mount/args))
        all-channels  (reduce (fn [all-channel-vec [topic-entity _]]
                                (concat all-channel-vec (get-keys-for-topic stream-routes topic-entity)))
                              []
                              stream-routes)]
    (or (pos? (count all-channels))
        (-> (ziggurat-config) :retry :enabled))))

(defn- channel-threads [channels]
  (reduce (fn [sum [_ channel-config]]
            (+ sum (:worker-count channel-config))) 0 channels))

(defn total-thread-count []
  (let [stream-routes                (:stream-router (ziggurat-config))
        batch-route-count            (count (:batch-routes (ziggurat-config)))
        worker-count                 (get-in (ziggurat-config) [:jobs :instant :worker-count] 0)
        batch-routes-instant-workers (* batch-route-count worker-count)]
    (reduce (fn [sum [_ route-config]]
              (+ sum (channel-threads (:channels route-config)) worker-count))
            batch-routes-instant-workers
            stream-routes)))

(defn create-rmq-connection
  [connection-factory rabbitmq-config]
  (.setCredentialsProvider connection-factory (DefaultCredentialsProvider. (:username rabbitmq-config) (:password rabbitmq-config)))
  (if (some? (:executor rabbitmq-config))
    (.newConnection connection-factory
                    ^ExecutorService (:executor rabbitmq-config)
                    ^AddressResolver (util/create-address-resolver rabbitmq-config)
                    (:connection-name rabbitmq-config))
    (.newConnection connection-factory nil ^AddressResolver
                    (util/create-address-resolver rabbitmq-config)
                    (:connection-name rabbitmq-config))))

(defn create-connection [config]
  (create-rmq-connection (ConnectionFactory.) config))

(defn- get-connection-config
  [is-producer?]
  (if is-producer?
    (assoc (:rabbit-mq-connection (ziggurat-config))
           :connection-name (str "producer-" (:app-name (ziggurat-config))))
    (assoc (:rabbit-mq-connection (ziggurat-config))
           :executor (Executors/newFixedThreadPool (total-thread-count))
           :connection-name (str "consumer-" (:app-name (ziggurat-config))))))

(defn start-connection
  "is-producer? - defines whether the connection is being created for producers or consumers
  producer connections do not require the :executor option"
  [is-producer?]
  (log/info "Connecting to RabbitMQ")
  (when (is-connection-required?)
    (try
      (let
       [connection (create-connection (get-connection-config is-producer?))]
        (log/info "Connection created " connection)
        (doto connection
          (.addShutdownListener
           (reify ShutdownListener
             (shutdownCompleted [_ cause]
               (when-not (.isInitiatedByApplication cause)
                 (log/error cause "RabbitMQ connection shut down due to error")))))))
      (catch Exception e
        (report-error e "Error while starting RabbitMQ connection")
        (throw e)))))

(defn stop-connection [conn]
  (when (is-connection-required?)
    (log/info "Closing the RabbitMQ connection")
    (rmq/close conn)))