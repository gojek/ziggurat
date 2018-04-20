(ns ziggurat.messaging.name
  (:require [ziggurat.config :refer [ziggurat-config]]))

(defn get-with-prepended-app-name [name]
  (format name (:app-name (ziggurat-config))))
