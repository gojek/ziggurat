(ns ziggurat.fixtures
  (:require [clojure.test :refer :all]
            [clojure.stacktrace :as st]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messaging.util :as util]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.server :refer [server]]
            [ziggurat.messaging.producer :as pr]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]))

(defn mount-config []
  (-> (mount/only [#'config/config])
      (mount/swap {#'config/config (config/make-config "config.test.edn")})
      (mount/start)))

(defn mount-only-config [f]
  (mount-config)
  (f)
  (mount/stop))

(defn- get-queue-name [queue-type]
  (:queue-name (queue-type (config/rabbitmq-config))))

(defn- get-exchange-name [exchange-type]
  (:exchange-name (exchange-type (config/rabbitmq-config))))

(defn delete-queues [stream-routes]
  (with-open [ch (lch/open connection)]
    (doseq [topic-entity (keys stream-routes)]
      (let [topic-identifier (name topic-entity)]
        (lq/delete ch (util/prefixed-queue-name topic-identifier (get-queue-name :instant)))
        (lq/delete ch (util/prefixed-queue-name topic-identifier (get-queue-name :dead-letter)))
        (lq/delete ch (pr/delay-queue-name topic-identifier (get-queue-name :delay) (:queue-timeout-ms (:delay (config/rabbitmq-config)))))))))

(defn delete-exchanges [stream-routes]
  (with-open [ch (lch/open connection)]
    (doseq [topic-entity (keys stream-routes)]
      (let [topic-identifier (name topic-entity)]
        (le/delete ch (util/prefixed-queue-name topic-identifier (get-exchange-name :instant)))
        (le/delete ch (util/prefixed-queue-name topic-identifier (get-exchange-name :dead-letter)))
        (le/delete ch (util/prefixed-queue-name topic-identifier (get-exchange-name :delay)))))))

(defn init-rabbit-mq [f]
  (let [stream-routes {:booking {:handler-fn #(constantly nil)}}]
    (mount-config)
    (mount/start (mount/only [#'connection]))
    (f)
    (mount/stop)))

(defn start-server [f]
  (mount-config)
  (mount/start (mount/only [#'server]))
  (f)
  (mount/stop))

(defmacro with-queues [stream-routes body]
  `(try
     (pr/make-queues ~stream-routes)
     ~body
     (catch Exception e#
       (st/print-stack-trace e#))
     (finally
       (delete-queues ~stream-routes)
       (delete-exchanges ~stream-routes))))
