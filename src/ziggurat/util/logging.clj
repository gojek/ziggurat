(ns ziggurat.util.logging
  (:import [org.apache.logging.log4j ThreadContext]
           [org.apache.logging.log4j LogManager Level]))

(defmacro with-context
  [context-map & body]
  `(try
     (doseq [[k# v#] ~context-map] (ThreadContext/put (name k#) (str v#)))
     ~@body
     (finally (doseq [[k# _#] ~context-map] (ThreadContext/remove (name k#))))))

(defn- set-log-level
  [log-level]
  (let [level (case log-level
                :debug Level/DEBUG
                :info  Level/INFO
                :warn  Level/WARN
                :error Level/ERROR
                :fatal Level/FATAL
                :trace Level/TRACE
                :off   Level/OFF
                Level/ALL)]
    (.setLevel (LogManager/getRootLogger) level)))

(def set-log-level-to-all (partial set-log-level :all))

(def set-log-level-to-debug (partial set-log-level :debug))

(def set-log-level-to-info (partial set-log-level :info))

(def set-log-level-to-warn (partial set-log-level :warn))

(def set-log-level-to-error (partial set-log-level :error))

(def set-log-level-to-fatal (partial set-log-level :fatal))

(def set-log-level-to-trace (partial set-log-level :trace))

(def set-log-level-to-off (partial set-log-level :off))
