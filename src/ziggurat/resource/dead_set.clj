(ns ziggurat.resource.dead-set
  (:require [clojure.spec.alpha :as s]
            [ziggurat.messaging.replay :as r]))

(defn- validate-count [count]
  (and (s/valid? int? count) (s/int-in-range? 0 Integer/MAX_VALUE count)))

(defn replay [{:keys [params]}]
  (if (validate-count (:count params))
    (do (r/replay (:count params))
        {:status 200
         :body   {:message "Requeued messages on the queue for retrying"}})
    {:status 400
     :body   {:error "Count should be the positive integer"}}))
