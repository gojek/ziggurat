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
      (mount/with-args {::stream-args  {:mapper-fn     (:mapper-fn fns-map)
                                        :stream-routes (:stream-routes fns-map)}
                        ::actor-routes actor-routes})
      (mount/start))
  (messaging-producer/make-queues (:stream-routes fns-map))
  ;; We want subscribers to start after creating queues on RabbitMQ.
  (messaging-consumer/start-subscribers (:mapper-fn fns-map) (:stream-routes fns-map))
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

(defn main-with-stream-router
  "The entry point for your lambda actor.
  stream-routes accepts list of map of your topic entity eg: [:booking {:mapper-fn (fn [message] :success)}]
  mapper-fn must return :success, :retry or :skip
  start-fn takes no parameters, and will be run on application startup.
  stop-fn takes no parameters, and will be run on application shutdown."
  ([start-fn stop-fn stream-router]
   (main-with-stream-router start-fn stop-fn stream-router []))
  ([start-fn stop-fn stream-router actor-routes]
   (try
     (add-shutdown-hook stop-fn)
     (start start-fn {:stream-routes stream-router} actor-routes)
     (catch Exception e
       (log/error e)
       (stop stop-fn)
       (System/exit 1)))))

(defn construct-default-stream-router
  [main-fn]
  [{:default {:handler-fn main-fn}}])

(defn main
  "The entry point for your lambda actor.
  main-fn must be a fn which accepts one parameter: the message from Kafka. It must return :success, :retry or :skip.
  start-fn takes no parameters, and will be run on application startup.
  stop-fn takes no parameters, and will be run on application shutdown."
  ([start-fn stop-fn main-fn]
   (main start-fn stop-fn main-fn []))
  ([start-fn stop-fn main-fn actor-routes]
   (main-with-stream-router start-fn stop-fn (construct-default-stream-router main-fn) actor-routes)))
