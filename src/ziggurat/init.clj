(ns ziggurat.init
  "Contains the entry point for your lambda actor."
  (:require [clojure.tools.logging :as log]
            [schema.core :as s]
            [lambda-common.metrics :as metrics]
            [mount.core :as mount :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config] :as config]
            [ziggurat.messaging.connection :as messaging-connection]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.nrepl-server :as nrepl-server]
            [ziggurat.server :as server]
            [ziggurat.streams :as streams]))

(defstate lambda-statsd-reporter
  :start (metrics/start-statsd-reporter (:datadog (ziggurat-config))
                                        (:env (ziggurat-config))
                                        (:app-name (ziggurat-config)))
  :stop (metrics/stop-statsd-reporter lambda-statsd-reporter))

(defn start
  "Starts up Ziggurat's state, and then calls the actor-start-fn."
  [actor-start-fn stream-routes actor-routes]
  (-> (mount/only #{#'config/config
                    #'lambda-statsd-reporter
                    #'messaging-connection/connection
                    #'server/server
                    #'nrepl-server/server
                    #'streams/stream})
      (mount/with-args {::stream-routes stream-routes
                        ::actor-routes  actor-routes})
      (mount/start))
  (messaging-producer/make-queues stream-routes)
  ;; We want subscribers to start after creating queues on RabbitMQ.
  (messaging-consumer/start-subscribers stream-routes)
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

(s/defschema StreamRoute
  {s/Any {:handler-fn (s/pred #(fn? %1))}})

(defn validate-stream-routes [stream-routes]
  (if (and (map? stream-routes)
           (not (empty? stream-routes)))
    (s/validate StreamRoute stream-routes)
    (throw (ex-info "Stream Routes must be a map" {:data stream-routes}))))

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
