(ns ziggurat.init
  "Contains the entry point for your lambda actor."
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

(defn start
  "Starts up Ziggurat's state, and then calls the actor-start-fn."
  [actor-start-fn fns-map actor-routes]
  (-> (mount/only #{#'config/config
                    #'lambda-statsd-reporter
                    #'messaging-connection/connection
                    #'server/server
                    #'nrepl-server/server
                    #'streams/stream})
      (mount/with-args {::stream-routes fns-map
                        ::actor-routes  actor-routes})
      (mount/start))
  (messaging-producer/make-queues fns-map)
  ;; We want subscribers to start after creating queues on RabbitMQ.
  (messaging-consumer/start-subscribers fns-map)
  (actor-start-fn))

(defn stop
  "Calls the actor-stop-fn, and then stops Ziggurat's state."
  [actor-stop-fn]
  (actor-stop-fn)
  (mount/stop #'config/config
              #'lambda-statsd-reporter
              #'messaging-connection/connection
              #'server/server
              #'nrepl-server/server
              #'streams/stream))

(defn- add-shutdown-hook [actor-stop-fn]
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. ^Runnable #(do (stop actor-stop-fn)
                            (shutdown-agents))
             "Shutdown-handler")))

(defn- validate-stream-route [stream-route]
  (and
    (not (nil? stream-route))
    (not-empty stream-route)
    (not (nil? (:handler-fn ((first (keys stream-route)) stream-route))))))

(defn validate-stream-routes [stream-routes]
  (if-not (and
        (not (nil? stream-routes))
        (not-empty stream-routes)
        (reduce (fn [value stream-route]
                  (and (validate-stream-route stream-route) value))
                true stream-routes))
    (throw (IllegalArgumentException. "Invalid stream routes"))))

(defn main
  "The entry point for your lambda actor.
  Accepts stream-routes as a list of map of your handler functions to topic entities eg: [{:booking {:handler-fn (fn [message] :success)}}]
  handler-fn must return :success, :retry or :skip
  start-fn takes no parameters, and will be run on application startup.
  stop-fn takes no parameters, and will be run on application shutdown."
  ([start-fn stop-fn stream-routes]
   (main start-fn stop-fn stream-routes []))
  ([start-fn stop-fn stream-routes actor-routes]
   (try
     (validate-stream-routes stream-routes)
     (add-shutdown-hook stop-fn)
     (start start-fn stream-routes actor-routes)
     (catch Exception e
       (log/error e)
       (stop stop-fn)
       (System/exit 1)))))
