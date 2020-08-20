(ns ziggurat.util.logging
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.logging.log4j ThreadContext)))

(defmacro with-context
  [context-map & body]
  `(try
     (doseq [[k# v#] ~context-map] (ThreadContext/put (name k#) (str v#)))
     ~@body
     (finally (doseq [[k# _#] ~context-map] (ThreadContext/remove (name k#))))))

(defn log-warn-colored [& message]
  (let [red      "\u001b[31m"
        reset    "\u001b[0m"
        msg-list (conj red message)]
    (apply log/warn msg-list)
    (log/warn reset "")))

