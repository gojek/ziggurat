(ns ziggurat.fixtures
  (:require [clojure.test :refer :all]
            [clojure.stacktrace :as st]
            [mount.core :as mount]
            [ziggurat.config :as config]
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

(defn init-rabbit-mq [f]
  (mount-config)
  (mount/start (mount/only [#'connection]))
  (pr/make-queues nil)
  (pr/make-queues [{:booking {:handler-fn #(constantly nil)}}])
  (f)
  (mount/stop))

(defn start-server [f]
  (mount-config)
  (mount/start (mount/only [#'server]))
  (f)
  (mount/stop))

(defn flush-rabbitmq []
  (let [{:keys [queue-name exchange-name dead-letter-exchange queue-timeout-ms]} (:delay (config/rabbitmq-config))
        delay-queue-name-with-topic-prefix (pr/delay-queue-name "booking" queue-name queue-timeout-ms)
        delay-queue-name (pr/delay-queue-name nil queue-name queue-timeout-ms)]
    (with-open [ch (lch/open connection)]
      (lq/purge ch (str "booking" "_" (:queue-name (:instant (config/rabbitmq-config)))))
      (lq/purge ch (str "booking" "_" (:queue-name (:dead-letter (config/rabbitmq-config)))))
      (lq/purge ch delay-queue-name-with-topic-prefix)
      (lq/purge ch (:queue-name (:instant (config/rabbitmq-config))))
      (lq/purge ch (:queue-name (:dead-letter (config/rabbitmq-config))))
      (lq/purge ch delay-queue-name))))

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