(ns ziggurat.messaging.producer-connection
  (:require [mount.core :refer [defstate]]
            [ziggurat.messaging.connection-helper :as connection-helper]
            [clojure.tools.logging :as log]))

(declare producer-connection)

(defstate producer-connection
  :start (do (log/info "Creating producer connection")
             (connection-helper/start-connection true))
  :stop (do (log/info "Stopping producer connection")
            (connection-helper/stop-connection producer-connection)))
