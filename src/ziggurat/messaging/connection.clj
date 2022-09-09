(ns ziggurat.messaging.connection
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq]
            [mount.core :as mount :refer [defstate start]]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.channel :refer [get-keys-for-topic]]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.messaging.util :as util]
            [clojure.string :as str]
            [ziggurat.util.error :refer [report-error]])
  (:import [com.rabbitmq.client ShutdownListener Address ListAddressResolver ConnectionFactory DnsRecordIpAddressResolver AddressResolver]
           [java.util.concurrent Executors ExecutorService]
           [io.opentracing.contrib.rabbitmq TracingConnectionFactory]
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
                    ^AddressResolver (util/create-address-resolver rabbitmq-config))
    (.newConnection connection-factory ^AddressResolver (util/create-address-resolver rabbitmq-config))))

(defn create-connection [config tracer-enabled]
  (if tracer-enabled
    (create-rmq-connection (TracingConnectionFactory. tracer) config)
    (create-rmq-connection (ConnectionFactory.) config)))

(defn- get-connection-config
  [is-producer?]
  (if is-producer?
    (assoc (:rabbit-mq-connection (ziggurat-config))
           :connection-name "producer")
    (assoc (:rabbit-mq-connection (ziggurat-config))
           :executor (Executors/newFixedThreadPool (total-thread-count))
           :connection-name "consumer")))

(defn- start-connection
  "is-producer? - defines whether the connection is being created for producers or consumers
  producer connections do not require the :executor option"
  [is-producer?]
  (log/info "Connecting to RabbitMQ")
  (when (is-connection-required?)
    (try
      (let
       [is-tracer-enabled? (get-in (ziggurat-config) [:tracer :enabled])
        connection         (create-connection (get-connection-config is-producer?) is-tracer-enabled?)]
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

(defn- stop-connection [conn]
  (when (is-connection-required?)
    (log/info "Closing the RabbitMQ connection")
    (rmq/close conn)))

(defstate consumer-connection
  :start (do (log/info "Creating consumer connection")
             (start-connection false))
  :stop (do (log/info "Stopping consume connection")
            (stop-connection consumer-connection)))

(defstate producer-connection
  :start (do (log/info "Creating producer connection")
             (start-connection true))
  :stop (do (log/info "Stopping producer connection")
            (stop-connection producer-connection)))
