(ns ziggurat.init
  "Contains the entry point for your application."
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [schema.core :as s]
            [ziggurat.config :refer [ziggurat-config] :as config]
            [ziggurat.messaging.connection :as messaging-connection :refer [connection]]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.nrepl-server :as nrepl-server]
            [ziggurat.producer :as producer :refer [kafka-producers]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.server :as server]
            [ziggurat.streams :as streams]
            [ziggurat.tracer :as tracer]
            [ziggurat.util.java-util :as util]
            [ziggurat.kafka-consumer.executor-service :as executor-service]
            [ziggurat.kafka-consumer.consumer-driver :as consumer-driver])
  (:gen-class
   :methods [^{:static true} [init [java.util.Map] void]]
   :name tech.gojek.ziggurat.internal.Init))

(defn- event-routes [args]
  (merge (:stream-routes args) (:batch-routes args)))

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
  (messaging-consumer/start-subscribers (get args :stream-routes) (get args :batch-routes)))

(defn- start-rabbitmq-producers [args]
  (start-rabbitmq-connection args)
  (messaging-producer/make-queues (event-routes args)))

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
  (mount/stop #'connection))

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

(defn start-batch-consumer [args]
  (-> (mount/only #{#'executor-service/thread-pool
                    #'consumer-driver/consumer-groups})
      (mount/with-args (:batch-routes args))
      (mount/start)))

(defn stop-batch-consumer []
  (-> (mount/only #{#'executor-service/thread-pool
                    #'consumer-driver/consumer-groups})
      (mount/stop)))

(def valid-modes-fns
  {:api-server     {:start-fn start-server :stop-fn stop-server}
   :stream-worker  {:start-fn start-stream :stop-fn stop-stream}
   :worker         {:start-fn start-workers :stop-fn stop-workers}
   :batch-worker   {:start-fn start-batch-consumer :stop-fn stop-batch-consumer}
   :management-api {:start-fn start-management-apis :stop-fn stop-management-apis}})

(defn- valid-modes []
  (keys valid-modes-fns))

(defn- all-modes-except-batch-consumer []
  (remove #(= % :batch-worker) (valid-modes)))

(defn- execute-function
  ([modes fnk]
   (execute-function modes fnk nil))
  ([modes fnk args]
   (doseq [mode (-> modes
                    (or (valid-modes))
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
              #'connection
              #'nrepl-server/server
              #'tracer/tracer))

(defn- validate-routes-against-config
  ([routes route-type configs]
   (doseq [route routes]
     (let [topic-entity (first route)
           route-config (-> configs
                            (get-in [route-type topic-entity]))]
       (when (nil? route-config)
         (throw (IllegalArgumentException. (str "Error! Route " topic-entity " isn't present in the " route-type " config"))))))))

(defn validate-stream-and-batch-routes-against-config
  [stream-routes batch-routes modes configs]
  (when (contains? (set modes) :stream-worker)
    (validate-routes-against-config stream-routes :stream-router configs))
  (when (contains? (set modes) :batch-worker)
    (validate-routes-against-config batch-routes :batch-routes configs)))

(defn start
  "Starts up Ziggurat's config, reporters, actor fn, rabbitmq connection and then streams, server etc"
  [actor-start-fn stream-routes batch-routes actor-routes modes]
  (start-common-states)
  (validate-stream-and-batch-routes-against-config stream-routes batch-routes modes (ziggurat-config))
  (actor-start-fn)
  (execute-function modes
                    :start-fn
                    {:actor-routes  actor-routes
                     :stream-routes stream-routes
                     :batch-routes  batch-routes}))

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

(s/defschema BatchRoute
  (s/conditional
   #(and (seq %)
         (map? %))
   {s/Keyword {:handler-fn (s/pred #(fn? %))}}))

(defn validate-routes [stream-routes batch-routes modes]
  (when (contains? (set modes) :stream-worker)
    (s/validate StreamRoute stream-routes))
  (when (contains? (set modes) :batch-worker)
    (s/validate BatchRoute batch-routes)))

(defn- derive-modes [stream-routes batch-routes actor-routes]
  (let [base-modes    [:management-api :worker]]
    (if (and (nil? stream-routes) (nil? batch-routes))
      (throw (IllegalArgumentException. "Either :stream-routes or :batch-routes should be present in init args")))
    (cond-> base-modes
      (some? stream-routes) (conj :stream-worker)
      (some? batch-routes) (conj :batch-worker)
      (some? actor-routes) (conj :api-server))))

(defn validate-modes [modes stream-routes batch-routes actor-routes]
  (let [derived-modes (if-not (empty? modes)
                        modes
                        (derive-modes stream-routes batch-routes actor-routes))
        invalid-modes (filter #(not (contains? (set (valid-modes)) %)) derived-modes)]
    (if (pos? (count invalid-modes))
      (throw (ex-info "Wrong modes argument passed" {:invalid-modes invalid-modes}))
      derived-modes)))

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
   (main {:start-fn start-fn :stop-fn stop-fn :stream-routes stream-routes :actor-routes actor-routes :modes (all-modes-except-batch-consumer)}))
  ([{:keys [start-fn stop-fn stream-routes batch-routes actor-routes modes]}]
   (try
     (let [derived-modes (validate-modes modes stream-routes batch-routes actor-routes)]
       (validate-routes stream-routes batch-routes derived-modes)
       (add-shutdown-hook stop-fn derived-modes)
       (start start-fn stream-routes batch-routes actor-routes derived-modes))
     (catch clojure.lang.ExceptionInfo e
       (log/error e)
       (System/exit 1))
     (catch Exception e
       (log/error e)
       (stop stop-fn modes)
       (System/exit 1)))))

(defn -init [args]
  (main (util/java-map->clojure-map args)))
