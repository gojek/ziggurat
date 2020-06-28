(ns ziggurat.init
  "Contains the entry point for your application."
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [schema.core :as s]
            [ziggurat.config :refer [ziggurat-config] :as config]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.nrepl-server :as nrepl-server]
            [ziggurat.producer :as producer :refer [kafka-producers]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.server :as server]
            [ziggurat.streams :as streams]
            [ziggurat.tracer :as tracer]
            [ziggurat.util.java-util :as util])
  (:gen-class
   :methods [^{:static true} [init [java.util.Map] void]]
   :name tech.gojek.ziggurat.internal.Init))

(defn- start*
  ([states]
   (start* states nil))
  ([states args]
   (-> (mount/only states)
       (mount/with-args args)
       (mount/start))))

(defn- start-messaging-connection [args]
  (messaging/start-connection config/config (:stream-routes args)))

(defn- start-messaging-consumers [args]
  (start-messaging-connection args)
  (messaging-consumer/start-subscribers (get args :stream-routes) (ziggurat-config)))

(defn- start-messaging-producers [args]
  (start-messaging-connection args)
  (messaging-producer/make-queues (get args :stream-routes)))

(defn start-kafka-producers []
  (start* #{#'kafka-producers}))

(defn start-kafka-streams [args]
  (start* #{#'streams/stream} args))

(defn start-stream [args]
  (start-kafka-producers)
  (start-messaging-producers args)
  (start-kafka-streams args))

(defn start-management-apis [args]
  (start-messaging-connection args)
  (start* #{#'server/server} (dissoc args :actor-routes)))

(defn start-server [args]
  (start-messaging-connection args)
  (start* #{#'server/server} args))

(defn start-workers [args]
  (start-kafka-producers)
  (start-messaging-producers args)
  (start-messaging-consumers args))

(defn- stop-messaging []
  (messaging/stop-connection config/config (:stream-routes mount/args)))

(defn stop-kafka-producers []
  (mount/stop #'kafka-producers))

(defn stop-kafka-streams []
  (mount/stop #'streams/stream))

(defn stop-workers []
  (stop-messaging)
  (stop-kafka-producers))

(defn stop-server []
  (mount/stop #'server/server)
  (stop-messaging))

(defn stop-stream []
  (stop-kafka-streams)
  (stop-messaging)
  (stop-kafka-producers))

(defn stop-management-apis []
  (mount/stop #'server/server)
  (stop-messaging))

(def valid-modes-fns
  {:api-server     {:start-fn start-server :stop-fn stop-server}
   :stream-worker  {:start-fn start-stream :stop-fn stop-stream}
   :worker         {:start-fn start-workers :stop-fn stop-workers}
   :management-api {:start-fn start-management-apis :stop-fn stop-management-apis}})

(defn- execute-function
  ([modes fnk]
   (execute-function modes fnk nil))
  ([modes fnk args]
   (doseq [mode (-> modes
                    (or (keys valid-modes-fns))
                    sort)]
     (if (nil? args)
       ((fnk (get valid-modes-fns mode)))
       ((fnk (get valid-modes-fns mode)) args)))))

(defn start-common-states []
  (start* #{#'config/config
            #'metrics/statsd-reporter
            #'sentry-reporter
            #'nrepl-server/server
            #'tracer/tracer}))

(defn stop-common-states []
  (mount/stop #'config/config
              #'metrics/statsd-reporter
              #'nrepl-server/server
              #'tracer/tracer)
  (stop-messaging))

(defn start
  "Starts up Ziggurat's config, reporters, actor fn, rabbitmq connection and then streams, server etc"
  [actor-start-fn stream-routes actor-routes modes]
  (start-common-states)
  (actor-start-fn)
  (execute-function modes
                    :start-fn
                    {:actor-routes  actor-routes
                     :stream-routes stream-routes}))

(defn stop
  "Calls the Ziggurat's state stop fns and then actor-stop-fn."
  [actor-stop-fn modes]
  (actor-stop-fn)
  (stop-common-states)
  (execute-function modes :stop-fn))

(defn- add-shutdown-hook [actor-stop-fn modes]
  (.addShutdownHook
   (Runtime/getRuntime)
   (Thread. ^Runnable #((stop actor-stop-fn modes)
                        (shutdown-agents))
            "Shutdown-handler")))

(declare StreamRoute)

(s/defschema StreamRoute
  (s/conditional
   #(and (seq %)
         (map? %))
   {s/Keyword {:handler-fn (s/pred #(fn? %))
               s/Keyword   (s/pred #(fn? %))}}))

(defn validate-stream-routes [stream-routes modes]
  (when (or (empty? modes) (contains? (set modes) :stream-worker))
    (s/validate StreamRoute stream-routes)))

(defn validate-modes [modes]
  (let [invalid-modes       (filter #(not (contains? (set (keys valid-modes-fns)) %)) modes)
        invalid-modes-count (count invalid-modes)]
    (when (pos? invalid-modes-count)
      (throw (ex-info "Wrong modes arguement passed - " {:invalid-modes invalid-modes})))))

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
   (main {:start-fn start-fn :stop-fn stop-fn :stream-routes stream-routes :actor-routes actor-routes}))
  ([{:keys [start-fn stop-fn stream-routes actor-routes modes]}]
   (try
     (validate-modes modes)
     (validate-stream-routes stream-routes modes)
     (add-shutdown-hook stop-fn modes)
     (start start-fn stream-routes actor-routes modes)
     (catch clojure.lang.ExceptionInfo e
       (log/error e)
       (System/exit 1))
     (catch Exception e
       (log/error e)
       (stop stop-fn modes)
       (System/exit 1)))))

(defn -init [args]
  (main (util/java-map->clojure-map args)))
