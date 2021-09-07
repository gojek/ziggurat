(ns ziggurat.kafka-consumer.consumer-driver
  (:require [cambium.core :as clog]
            [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.kafka-consumer.consumer :as ct]
            [ziggurat.kafka-consumer.consumer-handler :as ch]
            [ziggurat.kafka-consumer.executor-service :refer [thread-pool DEFAULT_THREAD_COUNT]]
            [ziggurat.metrics :as metrics])
  (:import (java.util.concurrent ExecutorService RejectedExecutionException)
           (org.apache.kafka.clients.consumer Consumer)))

(defn- start-polling-with-consumer
  [consumer init-arg topic-entity consumer-config]
  (let [message-poller (reify Runnable (run [_] (ch/poll-for-messages consumer (:handler-fn init-arg) topic-entity consumer-config)))]
    (when message-poller
      (try
        (.submit ^ExecutorService thread-pool message-poller)
        (catch RejectedExecutionException e
          (metrics/increment-count ["ziggurat.batch.consumption"] "thread-pool.task.rejected" 1 {:topic-entity (name topic-entity)})
          (log/error "message polling task was rejected by the threadpool" e))))))

(defn- start-consumers-per-group
  [topic-entity consumer-config init-arg]
  (let [thread-count (or (:thread-count consumer-config) DEFAULT_THREAD_COUNT)]
    (reduce (fn [consumers _]
              (let [consumer (ct/create-consumer topic-entity consumer-config)]
                (when consumer
                  (start-polling-with-consumer consumer init-arg topic-entity consumer-config)
                  (conj consumers consumer))))
            []
            (range thread-count))))

(defn- start-consumers [consumer-configs init-args]
  (log/info "Starting consumers")
  (reduce (fn [consumer-groups [topic-entity init-arg]]
            (if-some [consumer-config (get consumer-configs topic-entity)]
              (assoc consumer-groups topic-entity (start-consumers-per-group topic-entity consumer-config init-arg))
              consumer-groups))
          {}
          init-args))

(defn- stop-consumers [consumer-groups]
  (log/info "stopping consumers")
  (doseq [[topic-entity consumers] consumer-groups]
    (clog/info {:consumer-group topic-entity} (str "Stopping threads for consumer group: " topic-entity))
    (doseq [consumer consumers]
      (.wakeup ^Consumer consumer))))

(declare consumer-groups)

(defstate consumer-groups
  :start (start-consumers (:batch-routes (ziggurat-config)) (mount/args))
  :stop  (stop-consumers consumer-groups))
