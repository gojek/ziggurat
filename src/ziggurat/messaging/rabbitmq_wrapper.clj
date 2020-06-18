(ns ziggurat.messaging.rabbitmq-wrapper
  (:require [ziggurat.config :refer [get-in-config]]
            [mount.core :refer [defstate]]
            [sentry-clj.async :as sentry]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.retry :refer [with-retry]]
            [clojure.tools.logging :as log]
            [ziggurat.messaging.util :refer [is-connection-required?]]
            [clojure.pprint]
            [ziggurat.tracer :refer [tracer]]
            [langohr.consumers :as lcons]
            [langohr.channel :as lch]
            [ziggurat.messaging.rabbitmq.connection :as rmq-connection]
            [ziggurat.messaging.rabbitmq.producer :as rmq-producer]
            [ziggurat.messaging.rabbitmq.consumer :as rmq-consumer]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.core :as rmq]
            [mount.core :as mount])
  (:import (io.opentracing.contrib.rabbitmq TracingConnectionFactory)
           (com.rabbitmq.client ListAddressResolver Address)
           (com.rabbitmq.client.impl DefaultCredentialsProvider)
           (java.util.concurrent ExecutorService Executors)))

(defn start-connection [config stream-routes]
  (when (is-connection-required? (:ziggurat config) stream-routes)
    (rmq-connection/start-connection config)))

(defn stop-connection [connection config stream-routes]
  (when (is-connection-required? (:ziggurat config) stream-routes)
    (rmq-connection/stop-connection connection config)))



(defstate connection
          :start (start-connection ziggurat.config/config (:stream-routes (mount/args)))
          :stop (stop-connection connection ziggurat.config/config (:stream-routes (mount/args))))


(defn publish
  ([exchange message-payload]
    (publish exchange message-payload nil))
  ([exchange message-payload expiration]
    (rmq-producer/publish connection exchange message-payload expiration)))

(defn create-and-bind-queue
  ([queue-name exchange-name]
    (create-and-bind-queue queue-name exchange-name nil))
  ([queue-name exchange-name dead-letter-exchange]
    (rmq-producer/create-and-bind-queue connection queue-name exchange-name dead-letter-exchange)))


(defn get-messages-from-queue
  ([queue-name ack?]
   (get-messages-from-queue queue-name ack? 1))
  ([queue-name ack? count]
    (rmq-consumer/get-messages-from-queue connection queue-name ack? count)))

(defn process-messages-from-queue [queue-name count processing-fn]
  (rmq-consumer/process-messages-from-queue connection queue-name count processing-fn))

(defn start-subscriber [prefetch-count wrapped-mapper-fn queue-name]
  (rmq-consumer/start-subscriber connection prefetch-count wrapped-mapper-fn queue-name))

(defn consume-message [ch meta payload ack?]
  (rmq-consumer/consume-message ch meta payload ack?))

