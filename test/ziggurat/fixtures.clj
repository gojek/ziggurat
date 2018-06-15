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

(defn- delete-queues [stream-routes]
  (with-open [ch (lch/open connection)]
    (doseq [stream-route stream-routes]
      (let [topic-identifier (name (first (keys stream-route)))]
        (lq/delete ch (util/get-value-with-prefix-topic topic-identifier (get-queue-name :instant)))
        (lq/delete ch (util/get-value-with-prefix-topic topic-identifier (get-queue-name :dead-letter)))
        (lq/delete ch (pr/delay-queue-name topic-identifier (get-queue-name :delay) (:queue-timeout-ms (:delay (config/rabbitmq-config)))))))))

(defn init-rabbit-mq [f]
  (let [stream-routes [{:booking {:handler-fn #(constantly nil)}}]]
    (mount-config)
    (mount/start (mount/only [#'connection]))
    (pr/make-queues stream-routes)
    (f)
    (delete-queues stream-routes)
    (mount/stop)))

(defn start-server [f]
  (mount-config)
  (mount/start (mount/only [#'server]))
  (f)
  (mount/stop))

(defn flush-rabbitmq []
  (let [{:keys [queue-name queue-timeout-ms]} (:delay (config/rabbitmq-config))
        topic-identifier "booking"
        delay-queue-name-with-topic-prefix (pr/delay-queue-name topic-identifier queue-name queue-timeout-ms)
        instant-queue-name (util/get-value-with-prefix-topic topic-identifier (get-queue-name :instant))
        dead-letter-queue-name (util/get-value-with-prefix-topic topic-identifier (get-queue-name :dead-letter))]
    (with-open [ch (lch/open connection)]
      (lq/purge ch instant-queue-name)
      (lq/purge ch dead-letter-queue-name)
      (lq/purge ch delay-queue-name-with-topic-prefix))))

(defn clear-data []
  (flush-rabbitmq))

(defmacro with-clear-data [& body]
  `(try
     (clear-data)
     ~@body
     (catch Exception e#
       (st/print-stack-trace e#))
     (finally
       (clear-data))))