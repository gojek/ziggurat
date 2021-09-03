(ns ziggurat.util.logging
  (:require [cambium.core  :as clog]))

(defmacro with-context
  [context-map & body]
  `(clog/with-logging-context ~context-map ~@body))

