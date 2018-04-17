(ns ziggurat.nrepl-server
  (:require [clojure.tools.logging :as log]
            [clojure.tools.nrepl.server :as nrepl]
            [ziggurat.config :refer [ziggurat-config]]
            [mount.core :refer [defstate]]))

(defn- start []
  (let [port (-> (ziggurat-config) :nrepl-server :port)]
    (log/info "Starting nREPL server on port:" port)
    (nrepl/start-server :port port)))

(defn- stop [server]
  (nrepl/stop-server server)
  (log/info "Stopped nREPL server"))

(defstate server
  :start (start)
  :stop (stop server))
