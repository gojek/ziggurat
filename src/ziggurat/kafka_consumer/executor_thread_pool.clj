(ns ziggurat.kafka-consumer.executor-thread-pool
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log]
            [ziggurat.metrics :as metrics])
  (:import (java.util.concurrent Executors ExecutorService TimeUnit)))

(defn- total-thread-count
  [consumer-group-configs]
  (reduce-kv
   (fn [thread-count k v]
     (+ thread-count (:thread-count v)))
   0 consumer-group-configs))

(defn- construct-thread-pool
  [total-thread-count]
  (log/info "Creating thread pool with " total-thread-count " threads")
  (Executors/newFixedThreadPool total-thread-count))

(defn- shutdown
  [^ExecutorService executor-thread-pool]
  (log/info "Shutting down the thread pool for Kafka consumer")
  (try
    (.shutdown executor-thread-pool)
    (.awaitTermination executor-thread-pool 30000 (TimeUnit/MILLISECONDS))
    (catch Exception e
      (metrics/increment-count ["ziggurat.batch.consumption"] "thread-pool.shutdown" 1)
      (log/error e "Error while shutting down thread pool"))))

(defstate executor-thread-pool
  :start (construct-thread-pool (total-thread-count (:consumers (ziggurat-config))))
  :stop (shutdown executor-thread-pool))

