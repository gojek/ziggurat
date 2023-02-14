(ns ziggurat.server
  (:require [clojure.tools.logging :as log]
            [cheshire.generate :refer [add-encoder encode-str]]
            [mount.core :as mount :refer [defstate]]
            [ring.adapter.jetty :as ring]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.server.routes :as routes])
  (:import (java.util.concurrent TimeoutException)
           (org.eclipse.jetty.server Server)
           (java.time Instant)
           (org.eclipse.jetty.server.handler StatisticsHandler)))

(add-encoder Instant encode-str)

(def default-stop-timeout-ms 30000)

(defn configure-jetty [^Server server]
  (let [stats-handler   (StatisticsHandler.)
        timeout-ms      (get-in (ziggurat-config)
                                [:http-server :graceful-shutdown-timeout-ms]
                                default-stop-timeout-ms)
        default-handler (.getHandler server)]
    (.setHandler stats-handler default-handler)
    (.setHandler server stats-handler)
    (.setStopTimeout server timeout-ms)
    (.setStopAtShutdown server true)))

(defn- start [handler]
  (let [conf         (:http-server (ziggurat-config))
        port         (:port conf)
        thread-count (:thread-count conf)]
    (log/info "Starting server on port:" port)
    (ring/run-jetty handler {:port                 port
                             :min-threads          thread-count
                             :max-threads          thread-count
                             :join?                false
                             :send-server-version? false
                             :configurator         configure-jetty})))

(defn- stop [^Server server]
  (try
    (.stop server)
    (catch TimeoutException _ (log/info "Graceful shutdown timed out")))
  (log/info "Stopped server gracefully"))

(defstate server
  :start (start (routes/handler (:actor-routes (mount/args))))
  :stop (stop server))
