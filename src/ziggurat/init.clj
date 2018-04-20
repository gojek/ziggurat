(ns ziggurat.init
  (:require [ziggurat.config :refer [ziggurat-config] :as config]
            [lambda-common.metrics :as metrics]
            [mount.core :refer [defstate]]
            [ziggurat.messaging.connection :as messaging-connection]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.server :as server]
            [ziggurat.nrepl-server :as nrepl-server]
            [ziggurat.streams :as streams]
            [mount.core :as mount]
            [clojure.tools.logging :as log]))

(defstate lambda-statsd-reporter
  :start (metrics/start-statsd-reporter (:datadog (ziggurat-config))
                                        (:env (ziggurat-config))
                                        (:app-name (ziggurat-config)))
  :stop (metrics/stop-statsd-reporter lambda-statsd-reporter))

(defn start [actor-start-fn mapper-fn]
  (-> (mount/only #{#'config/config
                    #'lambda-statsd-reporter
                    #'messaging-connection/connection
                    #'server/server
                    #'nrepl-server/server
                    #'streams/stream})
      (mount/with-args {::mapper-fn mapper-fn})
      (mount/start))
  (messaging-producer/make-queues)
  ;; We want subscribers to start after creating queues on RabbitMQ.
  (messaging-consumer/start-subscribers)
  (actor-start-fn))

(defn stop [actor-stop-fn]
  (actor-stop-fn)
  (mount/stop #'config/config
              #'lambda-statsd-reporter
              #'messaging-connection/connection
              #'server/server
              #'nrepl-server/server
              #'streams/stream))

(defn add-shutdown-hook [actor-stop-fn]
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. ^Runnable  #(do (stop actor-stop-fn)
                             (shutdown-agents))
             "Shutdown-handler")))

(defn main [start-fn stop-fn main-fn]
  (try
    (add-shutdown-hook start-fn)
    (start start-fn main-fn)
    (catch Exception e
      (log/error e)
      (stop stop-fn)
      (System/exit 1))))
