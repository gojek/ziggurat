(ns ziggurat.messaging.producer_connection
  (:require [mount.core :refer [defstate]]
            [ziggurat.messaging.connection-helper :as connection-helper]))

(declare producer-connection)

(defstate producer-connection
  :start (do (log/info "Creating producer connection")
             (connection-helper/start-connection true))
  :stop (do (log/info "Stopping producer connection")
            (stop-connection producer-connection)))
