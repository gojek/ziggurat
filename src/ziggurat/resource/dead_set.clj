(ns ziggurat.resource.dead-set
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]
            [ziggurat.messaging.dead-set :as r]
            [mount.core :as mount]))

(defn- validate-count [count]
  (let [schema (s/constrained s/Num #(<= 0 % Integer/MAX_VALUE))]
    (try
      (s/validate schema count)
      (catch Throwable e
        false))))

(defn- parse-count [count]
  (try
    (Integer/parseInt count)
    (catch NumberFormatException ex
      (log/errorf "count %s is not an integer" count)
      nil)))

(defn- validate-params
  ([count topic-entity stream-routes]
   (validate-params count topic-entity stream-routes :handler-fn))
  ([count topic-entity stream-routes channel]
   (and (some? (get-in stream-routes [(keyword topic-entity) (keyword channel)]))
        (validate-count count))))

(defn get-replay []
  (let [stream-routes (:stream-routes (mount/args))]
    (fn [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (validate-params parsed-count topic-entity stream-routes)
          (do (r/replay parsed-count topic-entity)
              {:status 200
               :body   {:message "Requeued messages on the queue for retrying"}})
          {:status 400
           :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}})))))

(defn get-view []
  (let [stream-routes (:stream-routes (mount/args))]
    (fn view [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (validate-params parsed-count topic-entity stream-routes)
          {:status 200
           :body   {:messages (r/view parsed-count topic-entity)}}
          {:status 400
           :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}})))))
