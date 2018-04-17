(ns ziggurat.fixtures
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messsaging.connection :refer [connection]]
            [ziggurat.server :refer [server]]
            [ziggurat.messsaging.producer :as pr]))

(defn mount-config []
  (-> (mount/only [#'config/config])
      (mount/swap {#'config/config (config/make-config "config.test.edn")})
      (mount/start)))

(defn mount-test-config [f]
  (mount-config)
  (f)
  (mount/stop))

(defn init-rabbit-mq [f]
  (mount-config)
  (mount/start (mount/only [#'connection]))
  (pr/make-queues)
  (f)
  (mount/stop))

(defn start-server [f]
  (mount-config)
  (mount/start (mount/only [#'server]))
  (f)
  (mount/stop))

