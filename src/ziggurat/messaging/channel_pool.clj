(ns ziggurat.messaging.channel-pool
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :as zc :refer [ziggurat-config]]
            [ziggurat.messaging.producer-connection :as c]
            [ziggurat.messaging.connection-helper :as connection-helper]
            [cambium.core :as clog]
            [clojure.tools.logging :as log])
  (:import (com.rabbitmq.client Connection)
           (org.apache.commons.pool2.impl GenericObjectPool GenericObjectPoolConfig)
           (java.time Duration)
           (gojek.rabbitmq.channel_pool RabbitMQChannelFactory)))

(defn- validate-and-update-config
  "If min-idle for some reason is greater than max-idle then max-idle is replaced with min-idle"
  [config]
  (if (> (:min-idle config) (:max-idle config))
    (assoc config :max-idle (:min-idle config))
    config))

(defn calc-total-thread-count []
  (let [rmq-thread-count            (connection-helper/total-thread-count)
        stream-router-config        (get (zc/ziggurat-config) :stream-router)
        batch-routes-config         (get (zc/ziggurat-config) :batch-routes)
        batch-consumer-thread-count (reduce (fn [sum config]
                                              (+ sum (get config :thread-count 0))) 0 (vals batch-routes-config))
        stream-thread-count         (reduce (fn [sum config]
                                              (+ sum (get config :stream-threads-count 0))) 0 (vals stream-router-config))]
    (clog/info {:channel-threads        rmq-thread-count
                :batch-consumer-threads batch-consumer-thread-count
                :stream-threads         stream-thread-count} "Thread counts")
    (+ stream-thread-count rmq-thread-count batch-consumer-thread-count)))

(defn create-object-pool-config [config]
  (let [standby-size       10
        total-thread-count (calc-total-thread-count)
        default-config     {:max-wait-ms 5000 :min-idle standby-size :max-idle total-thread-count}
        merged-config      (->> config
                                (merge default-config)
                                validate-and-update-config)]
    (doto (GenericObjectPoolConfig.)
      (.setMaxWait (Duration/ofMillis (:max-wait-ms merged-config)))
      (.setMinIdle (:min-idle merged-config))
      (.setMaxIdle (:max-idle merged-config))
      (.setMaxTotal (+ (:min-idle merged-config) total-thread-count))
      (.setTestOnBorrow true)
      (.setJmxNamePrefix "zig-rabbitmq-ch-pool"))))

(defn create-channel-pool [^Connection connection]
  (let [pool-config   (create-object-pool-config
                       (get-in (ziggurat-config)
                               [:rabbit-mq-connection :channel-pool]))
        rmq-chan-pool (GenericObjectPool. (RabbitMQChannelFactory. connection) pool-config)]
    rmq-chan-pool))

(defn is-pool-alive? [channel-pool]
  (= (type channel-pool) GenericObjectPool))

(defn destroy-channel-pool [channel-pool]
  (.close channel-pool))

(declare channel-pool)

(defstate channel-pool
  :start (do (log/info "Creating channel pool")
             (create-channel-pool c/producer-connection))
  :stop (do (log/info "Stopping channel pool")
            (destroy-channel-pool channel-pool)))


