(ns ziggurat.messsaging.name
  (:require [ziggurat.config :refer [config]]))

(defn get-with-prepended-app-name [name]
  (format name (:app-name config)))
