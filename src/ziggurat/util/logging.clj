(ns ziggurat.util.logging
  (:import (org.apache.logging.log4j ThreadContext)))

(defmacro with-context
  [context-map & body]
  `(try
     (doseq [[k# v#] ~context-map] (ThreadContext/put (name k#) (str v#)))
     ~@body
     (finally (doseq [[k# _#] ~context-map] (ThreadContext/remove (name k#))))))
