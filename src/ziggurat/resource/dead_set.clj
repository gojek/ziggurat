(ns ziggurat.resource.dead-set
  (:require [clojure.spec.alpha :as s]
            [ziggurat.messaging.dead-set :as r]
            [clojure.tools.logging :as log]))

(defn- validate-count [count]
  (and (s/valid? int? count) (s/int-in-range? 0 Integer/MAX_VALUE count)))

(defn- parse-count [count]
  (try
    (Integer/parseInt count)
    (catch NumberFormatException ex
      (log/errorf "count %s is not an integer" count)
      nil)))

(defn replay [{:keys [params]}]
  (let [count        (:count params)
        topic-entity (:topic-entity params)]
    (if (and (validate-count count) (not (nil? topic-entity)))
      (do (r/replay count topic-entity)
          {:status 200
           :body   {:message "Requeued messages on the queue for retrying"}})
      {:status 400
       :body   {:error "Count should be the positive integer and topic entity should be present"}})))

(defn- validate [count topic-entity]
  (if (and (validate-count count) (not (nil? topic-entity)))
    count
    nil))

(defn view [{:keys [params]}]
  (if-let [count (validate (parse-count (:count params)) (:topic-entity params))]
    (if-let [messages (r/view count (:topic-entity params))]
      {:status 200
       :body   {:messages messages}})
    {:status 400
     :body   {:error "Count should be the positive integer and topic entity should be present"}}))