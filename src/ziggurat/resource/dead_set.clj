(ns ziggurat.resource.dead-set
  (:require [schema.core :as spec]
            [clojure.tools.logging :as log]
            [ziggurat.messaging.dead-set :as r]))

(defn- validate-count [count]
  (let [schema (spec/constrained spec/Num #(<= 0 % Integer/MAX_VALUE))]
    (try
      (spec/validate schema count)
      (catch Throwable e
        false))))

(defn- parse-count [count]
  (try
    (Integer/parseInt count)
    (catch NumberFormatException ex
      (log/errorf "count %s is not an integer" count)
      nil)))

(defn- validate-params [count topic-entity]
  (when (and (validate-count count) (some? topic-entity))
    count))

(defn replay [{{:keys [count topic-entity]} :params}]
  (if (validate-params count topic-entity)
    (do (r/replay count topic-entity)
        {:status 200
         :body   {:message "Requeued messages on the queue for retrying"}})
    {:status 400
     :body   {:error "Count should be the positive integer and topic entity should be present"}}))

(defn view [{{:keys [count topic-entity]} :params}]
  (if-let [count (validate-params (parse-count count) topic-entity)]
    {:status 200
     :body   {:messages (r/view count topic-entity)}}
    {:status 400
     :body   {:error "Count should be the positive integer and topic entity should be present"}}))
