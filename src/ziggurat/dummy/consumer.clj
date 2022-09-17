(ns ziggurat.dummy.consumer
  (:require [mount.core :as mount :refer [defstate]]
            [ziggurat.dummy.producer-connection]
            [ziggurat.dummy.consumer-connection]
            [ziggurat.dummy.producer]))

(declare my-consumers)

(defstate my-consumers
          :start (do (println "starting consumers") (Thread/sleep 3000))
          :stop (println "stopping consumers"))

