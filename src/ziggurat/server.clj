(ns ziggurat.server
  (:require [ziggurat.config :refer [config]]
            [ziggurat.server.routes :as routes]
            [cheshire.generate :refer [add-encoder encode-str]]
            [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [ring.adapter.jetty :as ring])
  (:import (org.eclipse.jetty.server Server)
           (java.time Instant)))

(add-encoder Instant encode-str)

(defn- start [handler]
  (let [conf (:http-server config)
        port (:port conf)
        thread-count (:thread-count conf)]
    (log/info "Starting server on port:" port)
    (ring/run-jetty handler {:port                 port
                             :min-threads          thread-count
                             :max-threads          thread-count
                             :join?                false
                             :send-server-version? false})))

(defn- stop [^Server server]
  (.stop server)
  (log/info "Stopped server"))

(defstate server
  :start (start (routes/handler))
  :stop (stop server))
