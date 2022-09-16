(ns ziggurat.messaging.consumer-connection
  (:require [mount.core :refer [defstate]]
            [ziggurat.messaging.connection-helper :as connection-helper]))

(declare consumer-connection)

(defstate consumer-connection
          :start (do (log/info "Creating consumer connection")
                     (connection-helper/start-connection false))
          :stop (do (log/info "Stopping consume connection")
                    (connection-helper/stop-connection consumer-connection)))
