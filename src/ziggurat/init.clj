(ns ziggurat.init
  "Contains the entry point for your application."
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [schema.core :as s]
            [ziggurat.config :refer [ziggurat-config] :as config]
            [ziggurat.metrics :as metrics]
            [ziggurat.messaging.connection :as messaging-connection]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.nrepl-server :as nrepl-server]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.server :as server]
            [ziggurat.streams :as streams]))

(defstate statsd-reporter
  :start (metrics/start-statsd-reporter (:datadog (ziggurat-config))
                                        (:env (ziggurat-config))
                                        (:app-name (ziggurat-config)))
  :stop (metrics/stop-statsd-reporter statsd-reporter))

(defn- start*
  ([states]
   (start* states nil))
  ([states args]
   (-> (mount/only states)
       (mount/with-args args)
       (mount/start))))

(defn start-rabbitmq-connection [stream-routes]
  (start* #{#'messaging-connection/connection} {:stream-routes stream-routes}))

(defn stop-rabbitmq-connection []
  (mount/stop #'messaging-connection/connection))

(defn start-rabbitmq-consumers [stream-routes]
  (start-rabbitmq-connection stream-routes)
  (messaging-consumer/start-subscribers stream-routes))

(defn start-rabbitmq-producers [stream-routes]
  (start-rabbitmq-connection stream-routes)
  (messaging-producer/make-queues stream-routes))

(defn start-stream [stream-routes]
  (start-rabbitmq-producers stream-routes)
  (start* #{#'streams/stream} stream-routes))

(defn start-management-apis [stream-routes]
  (start-rabbitmq-connection stream-routes)
  (start* #{#'server/server} {:stream-routes stream-routes}))

(defn start-server [stream-routes actor-routes]
  (start-rabbitmq-connection stream-routes)
  (start* #{#'server/server} {:stream-routes stream-routes
                              :actor-routes  actor-routes}))

(defn start-common-states []
  (start* #{#'config/config
            #'statsd-reporter
            #'sentry-reporter
            #'nrepl-server/server}))

(defn stop-common-states []
  (mount/stop #'config/config
              #'statsd-reporter
              #'messaging-connection/connection
              #'server/server
              #'nrepl-server/server
              #'streams/stream))

(defn stop-server []
  (mount/stop #'server/server)
  (stop-rabbitmq-connection))

(defn stop-stream []
  (mount/stop #'streams/stream)
  (stop-rabbitmq-connection))

(defn stop-management-apis []
  (mount/stop #'server/server)
  (stop-rabbitmq-connection))

(defn start
  "Starts up Ziggurat's config, reporters, actor fn, rabbitmq connection and then streams, server etc"
  [actor-start-fn stream-routes actor-routes]
  (start-common-states)
  (actor-start-fn)
  (start-stream stream-routes)
  (start-server stream-routes actor-routes)
  (start-rabbitmq-consumers stream-routes))     ;; We want subscribers to start after creating queues on RabbitMQ.


(defn stop
  "Calls the Ziggurat's state stop fns and then actor-stop-fn."
  [actor-stop-fn]
  (stop-common-states)
  (stop-stream)
  (stop-server)
  (actor-stop-fn)
  (mount/stop #'config/config))

(defn- add-shutdown-hook [actor-stop-fn]
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. ^Runnable #(do (stop actor-stop-fn)
                            (shutdown-agents))
             "Shutdown-handler")))

(s/defschema StreamRoute
  (s/conditional
    #(and (seq %)
          (map? %))
    {s/Keyword {:handler-fn (s/pred #(fn? %))
                s/Keyword   (s/pred #(fn? %))}}))

(defn validate-stream-routes [stream-routes]
  (s/validate StreamRoute stream-routes))

(defn main
  "The entry point for your application.

  Accepts stream-routes as a nested map keyed by the topic entities.
  Each topic entity is a map with a handler-fn described. For eg.,

  {:default {:handler-fn (fn [message] :success)}}
  :handler-fn must return :success, :retry or :skip

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
