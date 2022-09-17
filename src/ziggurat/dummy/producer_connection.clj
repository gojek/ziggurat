(ns ziggurat.dummy.producer-connection
  (:require [mount.core :as mount :refer [defstate]]))

(declare my-producer-connection)

(defstate my-producer-connection
          :start (do (println "starting producer connection")(Thread/sleep 5000))
          :stop (println "stopping producer connection"))
