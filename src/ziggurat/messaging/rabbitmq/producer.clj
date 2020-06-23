(ns ziggurat.messaging.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [langohr.exchange :as le]
            [ziggurat.messaging.rabbitmq.retry :refer :all]
            [langohr.queue :as lq]
            [clojure.set :as set])
  (:import (org.apache.kafka.common.header.internals RecordHeader)))

(defn- record-headers->map [record-headers]
  (reduce (fn [header-map record-header]
            (assoc header-map (.key record-header) (String. (.value record-header))))
          {}
          record-headers))

(defn- properties-for-publish
  [expiration headers]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :headers      (record-headers->map headers)}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn publish
  ([connection exchange message-payload expiration]
   (try
     (with-retry {:count      5
                  :wait       100
                  :on-failure #(log/error "publishing message to rabbitmq failed with error " (.getMessage %))}
       (with-open [ch (lch/open connection)]
         (lb/publish ch exchange "" (nippy/freeze (dissoc message-payload :headers))
                     (properties-for-publish expiration (:headers message-payload)))))
     (catch Throwable e
       (log/error e "Pushing message to rabbitmq failed, data: " message-payload)
       (throw (ex-info "Pushing message to rabbitMQ failed after retries, data: " {:type  :rabbitmq-publish-failure
                                                                                   :error e}))))))

(defn- declare-exchange [ch exchange]
  (le/declare ch exchange "fanout" {:durable true :auto-delete false})
  (log/info "Declared exchange - " exchange))

(defn- create-queue [queue props ch]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

(defn- bind-queue-to-exchange [ch queue exchange]
  (lq/bind ch queue exchange)
  (log/infof "Bound queue %s to exchange %s" queue exchange))

(defn create-and-bind-queue
  ([connection queue-name exchange-name dead-letter-exchange]
   (try
     (let [props (if dead-letter-exchange
                   {"x-dead-letter-exchange" dead-letter-exchange}
                   {})]
       (let [ch (lch/open connection)]
         (create-queue queue-name props ch)
         (declare-exchange ch exchange-name)
         (bind-queue-to-exchange ch queue-name exchange-name)))
     (catch Exception e
       (log/error e "Error while declaring RabbitMQ queues")
       (throw e)))))
