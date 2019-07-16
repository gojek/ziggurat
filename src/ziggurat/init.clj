(ns ziggurat.init
  "Contains the entry point for your application."
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [schema.core :as s]
            [ziggurat.config :refer [ziggurat-config] :as config]
            [ziggurat.messaging.connection :as messaging-connection]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.nrepl-server :as nrepl-server]
            [ziggurat.producer :as producer :refer [kafka-producers]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.server :as server]
            [ziggurat.streams :as streams])
  (:gen-class
    :name tech.gojek.ziggurat.Init
    :methods [^{:static true} [javaMain [clojure.lang.IPersistentMap] void]]))

(defstate statsd-reporter
  :start (metrics/start-statsd-reporter (:datadog (ziggurat-config))
                                        (:env (ziggurat-config)))
  :stop (metrics/stop-statsd-reporter statsd-reporter))

(defn- start*
  ([states]
   (start* states nil))
  ([states args]
   (-> (mount/only states)
       (mount/with-args args)
       (mount/start))))

(defn- start-rabbitmq-connection [args]
  (start* #{#'messaging-connection/connection} args))

(defn- start-rabbitmq-consumers [args]
  (start-rabbitmq-connection args)
  (messaging-consumer/start-subscribers (get args :stream-routes)))

(defn- start-rabbitmq-producers [args]
  (start-rabbitmq-connection args)
  (messaging-producer/make-queues (get args :stream-routes)))

(defn start-kafka-producers []
  (start* #{#'kafka-producers}))

(defn start-kafka-streams [args]
  (start* #{#'streams/stream} args))

(defn start-stream [args]
  (start-kafka-producers)
  (start-rabbitmq-producers args)
  (start-kafka-streams args))

(defn start-management-apis [args]
  (start-rabbitmq-connection args)
  (start* #{#'server/server} (dissoc args :actor-routes)))

(defn start-server [args]
  (start-rabbitmq-connection args)
  (start* #{#'server/server} args))

(defn start-workers [args]
  (start-kafka-producers)
  (start-rabbitmq-producers args)
  (start-rabbitmq-consumers args))

(defn- stop-rabbitmq-connection []
  (mount/stop #'messaging-connection/connection))

(defn stop-kafka-producers []
  (mount/stop #'kafka-producers))

(defn stop-kafka-streams []
  (mount/stop #'streams/stream))

(defn stop-workers []
  (stop-rabbitmq-connection)
  (stop-kafka-producers))

(defn stop-server []
  (mount/stop #'server/server)
  (stop-rabbitmq-connection))

(defn stop-stream []
  (stop-kafka-streams)
  (stop-rabbitmq-connection)
  (stop-kafka-producers))

(defn stop-management-apis []
  (mount/stop #'server/server)
  (stop-rabbitmq-connection))

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
            #'statsd-reporter
            #'sentry-reporter
            #'nrepl-server/server}))

(defn stop-common-states []
  (mount/stop #'config/config
              #'statsd-reporter
              #'messaging-connection/connection
              #'nrepl-server/server))

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
   (Thread. ^Runnable #(do (stop actor-stop-fn modes)
                           (shutdown-agents))
            "Shutdown-handler")))

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


(defn -javaMain
  [{:keys [start-fn stop-fn stream-routes actor-routes modes]}]
  (main {:start-fn start-fn
         :stop-fn stop-fn
         :stream-routes stream-routes
         :actor-routes actor-routes
         :modes modes}))

