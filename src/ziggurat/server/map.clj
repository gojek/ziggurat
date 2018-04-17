(ns ziggurat.server.map
  (:require [clojure.walk :as w]
            [medley.core :as m]))

(defn nested-map-keys [f m]
  (w/postwalk
    #(if (map? %)
       (m/map-keys f %)
       %)
    m))
