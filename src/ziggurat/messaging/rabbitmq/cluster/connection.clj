(ns ziggurat.messaging.rabbitmq.cluster.connection
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.tracer :refer [tracer]]
            [clojure.string :as str]
            [ziggurat.channel :refer [get-keys-for-topic]]
            [langohr.core :as rmq])
  (:import (java.util.concurrent ExecutorService Executors)
           (com.rabbitmq.client ListAddressResolver ShutdownListener Address)
           (com.rabbitmq.client.impl DefaultCredentialsProvider)
           (io.opentracing.contrib.rabbitmq TracingConnectionFactory)))

(defn- channel-threads [channels]
  (reduce (fn [sum [_ channel-config]]
            (+ sum (:worker-count channel-config))) 0 channels))

(defn transform-host-str [host-str port tracer-enabled?]
  (let [hosts (str/split host-str #",")]
    (if tracer-enabled?
      (map #(Address. % port) hosts)
      hosts)))

(defn- total-thread-count [ziggurat-config]
  (let [stream-routes (:stream-router ziggurat-config)
        worker-count  (get-in ziggurat-config [:jobs :instant :worker-count])]
    (reduce (fn [sum [_ route-config]]
              (+ sum (channel-threads (:channels route-config)) worker-count)) 0 stream-routes)))

(defn- get-config-for-rabbitmq [{ziggurat-config :ziggurat}]
  (assoc (:rabbit-mq-connection ziggurat-config) :executor (Executors/newFixedThreadPool (total-thread-count ziggurat-config))))

(defn- create-traced-clustered-connection [config]
  (let [connection-factory (TracingConnectionFactory. tracer)]
    (.setCredentialsProvider connection-factory (DefaultCredentialsProvider. (:username config) (:password config)))
    (.newConnection connection-factory ^ExecutorService (:executor config) ^ListAddressResolver (ListAddressResolver. (:hosts config)))))

(defn create-connection [config tracer-enabled]
  (if tracer-enabled
    (create-traced-clustered-connection (update-in config [:hosts]  transform-host-str (:port config) tracer-enabled))
    (rmq/connect (update-in config [:hosts]  transform-host-str (:port config) tracer-enabled))))

(defn start-connection [config]
  (log/info "Connecting to RabbitMQ")
  (try
    (let [connection (create-connection (get-config-for-rabbitmq config) (get-in config [:ziggurat :tracer :enabled]))]
      (doto connection
        (.addShutdownListener
         (reify ShutdownListener
           (shutdownCompleted [_ cause]
             (when-not (.isInitiatedByApplication cause)
               (log/error cause "RabbitMQ connection shut down due to error")))))))
    (catch Exception e
      (log/error e "Error while starting RabbitMQ connection")
      (throw e))))

(defn stop-connection [conn config]
  (if (get-in config [:ziggurat :tracer :enabled])
    (.close conn)
    (rmq/close conn))
  (log/info "Disconnected from RabbitMQ"))

