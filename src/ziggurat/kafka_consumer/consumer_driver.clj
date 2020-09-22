(ns ziggurat.kafka-consumer.consumer-driver
  (:require [mount.core :refer [defstate]]
            [ziggurat.kafka-consumer.consumer :as ct]
            [ziggurat.kafka-consumer.consumer-handler :as ch]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log]
            [ziggurat.kafka-consumer.executor-service :refer :all]
            [mount.core :as mount]
            [ziggurat.metrics :as metrics])
  (:import (java.util.concurrent ExecutorService RejectedExecutionException)
           (org.apache.kafka.clients.consumer Consumer)))

(defn- start-polling-with-consumer
  [consumer init-arg topic-entity consumer-config]
  (let [message-poller (cast Runnable #(ch/poll-for-messages consumer (:handler-fn init-arg) topic-entity consumer-config))]
    (when message-poller
      (try
        (.submit ^ExecutorService thread-pool ^Runnable message-poller)
        (catch RejectedExecutionException e
          (metrics/increment-count ["ziggurat.batch.consumption"] "thread-pool.task.rejected" 1 {:topic-entity topic-entity})
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
  (reduce (fn [consumer-groups [topic-entity consumer-config]]
            (let [init-arg-for-the-consumer-group (get init-args topic-entity)]
              (assoc consumer-groups topic-entity (start-consumers-per-group topic-entity consumer-config init-arg-for-the-consumer-group))))
          {}
          consumer-configs))

(defn- stop-consumers [consumer-groups]
  (do (log/info "stopping consumers")
      (doseq [[topic-entity consumers] consumer-groups]
        (log/info "Stopping threads for consumer group: " topic-entity)
        (doseq [consumer consumers]
          (.wakeup ^Consumer consumer)))))

(defstate consumer-groups
  :start (start-consumers (:batch-routes (ziggurat-config)) (mount/args))
  :stop  (stop-consumers consumer-groups))
