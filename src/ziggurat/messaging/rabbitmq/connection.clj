(ns ziggurat.messaging.rabbitmq.connection
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.channel :refer [get-keys-for-topic]]
            [langohr.core :as rmq])
  (:import (java.util.concurrent ExecutorService Executors)
           (com.rabbitmq.client ListAddressResolver ShutdownListener Address)
           (com.rabbitmq.client.impl DefaultCredentialsProvider)
           (io.opentracing.contrib.rabbitmq TracingConnectionFactory)))

(defn- channel-threads [channels]
  (reduce (fn [sum [_ channel-config]]
            (+ sum (:worker-count channel-config))) 0 channels))

(defn- total-thread-count [ziggurat-config]
  (let [stream-routes (:stream-router ziggurat-config)
        worker-count  (get-in ziggurat-config [:jobs :instant :worker-count])]
    (reduce (fn [sum [_ route-config]]
              (+ sum (channel-threads (:channels route-config)) worker-count)) 0 stream-routes)))

(defn- get-config-for-rabbitmq [{ziggurat-config :ziggurat}]
  (assoc (:rabbit-mq-connection ziggurat-config) :executor (Executors/newFixedThreadPool (total-thread-count ziggurat-config))))

(defn create-connection [config tracer-enabled]
  (if tracer-enabled
    (let [connection-factory (TracingConnectionFactory. tracer)]
      (.setCredentialsProvider connection-factory (DefaultCredentialsProvider. (:username config) (:password config)))
      (.newConnection connection-factory ^ExecutorService (:executor config) ^ListAddressResolver (ListAddressResolver. (list (Address. (:host config) (:port config))))))
    (rmq/connect config)))

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

