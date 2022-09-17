(ns ziggurat.dummy.channel-pool
  (:require [mount.core :as mount :refer [defstate]]
            [ziggurat.dummy.producer-connection]))

(declare my-cp)

(defstate my-cp
          :start (println "starting channel pool")
          :stop (println "stopping channel pool"))
