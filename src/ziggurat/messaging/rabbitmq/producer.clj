(ns ziggurat.messaging.rabbitmq.producer
  (:require [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [langohr.exchange :as le]
            [ziggurat.metrics :as metrics]
            [langohr.queue :as lq])
  (:import (com.rabbitmq.client AlreadyClosedException)
           (java.io IOException)))

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

(defn- handle-network-exception
  [e message-payload]
  (log/error e "Exception was encountered while publishing to RabbitMQ")
  (metrics/increment-count
   ["rabbitmq" "publish" "network"] "exception"
   {:topic-entity (name (:topic-entity message-payload))})
  true)

(defn publish-internal
  [connection exchange message-payload expiration]
  (try
    (with-open [ch (lch/open connection)]
      (lb/publish ch exchange "" (nippy/freeze (dissoc message-payload :headers))
                  (properties-for-publish expiration (:headers message-payload)))
      false)
    (catch AlreadyClosedException e
      (handle-network-exception e message-payload))
    (catch IOException e
      (handle-network-exception e message-payload))
    (catch Exception e
      (log/error e "Exception was encountered while publishing to RabbitMQ")
      (metrics/increment-count
       ["rabbitmq" "publish"] "exception"
       {:topic-entity (name (:topic-entity message-payload))})
      false)))

(defn publish
  ([connection exchange message-payload expiration]
   (when (publish-internal connection exchange message-payload expiration)
     (Thread/sleep 5000)
     (log/info "Retrying publishing the message to " exchange)
     (recur connection exchange message-payload expiration))))

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
