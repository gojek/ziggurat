(ns ziggurat.dummy.consumer
  (:require [mount.core :as mount :refer [defstate]]
            [ziggurat.dummy.producer-connection]
            [ziggurat.dummy.consumer-connection]
            [ziggurat.dummy.channel-pool]))

(declare my-consumers)

(defstate my-consumers
          :start (println "starting consumers")
          :stop (println "stopping consumers"))

