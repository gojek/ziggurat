(ns ziggurat.messaging.channel_pool
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :as zc]
            [ziggurat.messaging.connection :as c]
            [clojure.tools.logging :as log])
  (:import (com.rabbitmq.client Connection)
           (org.apache.commons.pool2.impl GenericObjectPool GenericObjectPoolConfig)
           (gojek.rabbitmq.channel_pool RabbitMQChannelFactory)
           (java.time Duration)))

(def default-config
  {:max-wait-ms 5000
   :min-idle    8
   :max-idle    8})

(defn calc-total-thread-count []
  (let [buffer-threads       10
        channel-thread-count (c/total-thread-count)

        stream-router-config (get (zc/ziggurat-config) :stream-router)
        stream-threads-count (reduce (fn [sum config]
                                       (+ sum (:stream-threads-count config))) 0 (vals stream-router-config))]
    (+ stream-threads-count channel-thread-count buffer-threads)))

(defn create-object-pool-config [config]
  (let [merged-config (merge config default-config)]
    (println merged-config)
    (doto (GenericObjectPoolConfig.)
      (.setMaxWait (Duration/ofMillis (get merged-config :max-wait-ms)))
      (.setMinIdle (get merged-config :min-idle))
      (.setMaxIdle (get merged-config :max-idle))
      (.setMaxTotal (calc-total-thread-count)))))

(defn create-channel-pool [^Connection connection]
  (let [pool-config   (create-object-pool-config (get-in zc/ziggurat-config [:rabbit-mq-connection :channel-pool]))
        rmq-chan-pool (GenericObjectPool. (RabbitMQChannelFactory. connection) pool-config)]
    rmq-chan-pool))

(defstate channel-pool
  :start (do (log/info "Creating channel pool")
             (create-channel-pool c/connection))
  :stop (do (log/info "Stopping channel pool")
            (.close channel-pool)))
