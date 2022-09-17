(ns ziggurat.dummy.channel-pool
  (:require [mount.core :as mount :refer [defstate]]
            [ziggurat.dummy.producer-connection]))

(declare my-cp)

(defstate my-cp
          :start (do (println "starting channel pool") (Thread/sleep 2000))
          :stop (println "stopping channel pool"))
