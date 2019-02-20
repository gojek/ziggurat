(ns ziggurat.util.map
  (:require [clojure.walk :as w]
            [medley.core :as m]))

(defn deep-merge [a b]
  (merge-with (fn [x y]
                (cond (map? y) (deep-merge x y)
                      (nil? x) y
                      :else x))
              a b))

(defn nested-map-keys [f m]
  (w/postwalk
   #(if (map? %)
      (m/map-keys f %)
      %)
   m))
