(ns ziggurat.kafka-consumer.executor-service
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log]
            [ziggurat.metrics :as metrics])
  (:import (java.util.concurrent Executors ExecutorService TimeUnit)))

(def DEFAULT_THREAD_COUNT 2)

(defn- total-thread-count
  [consumer-group-configs]
  (reduce
   (fn [thread-count config]
     (+ thread-count (or (:thread-count config) DEFAULT_THREAD_COUNT)))
   0 (vals consumer-group-configs)))

(defn- construct-thread-pool
  [total-thread-count]
  (log/info "Creating thread pool with " total-thread-count " threads")
  (Executors/newFixedThreadPool total-thread-count))

(defn- shutdown
  [^ExecutorService thread-pool]
  (log/info "Shutting down the thread pool for Kafka consumer")
  (try
    (.shutdown thread-pool)
    (.awaitTermination thread-pool 30000 (TimeUnit/MILLISECONDS))
    (catch Exception e
      (metrics/increment-count ["ziggurat.batch.consumption"] "thread-pool.shutdown" 1)
      (log/error e "Error while shutting down thread pool"))))

(defstate thread-pool
  :start (construct-thread-pool (total-thread-count (:batch-routes (ziggurat-config))))
  :stop (shutdown thread-pool))

