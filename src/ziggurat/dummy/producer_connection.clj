(ns ziggurat.dummy.producer-connection
  (:require [mount.core :as mount :refer [defstate]]))

(declare my-producer-connection)

(defstate my-producer-connection
          :start (println "starting producer connection")
          :stop (println "stopping producer connection"))
