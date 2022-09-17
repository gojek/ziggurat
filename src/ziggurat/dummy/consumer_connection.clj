(ns ziggurat.dummy.consumer-connection  
  (:require [mount.core :as mount :refer [defstate]]))

(declare my-consumer-connection)

(defstate my-consumer-connection
          :start (println "starting consumer connection")
          :stop (println "stopping consumer connection"))
