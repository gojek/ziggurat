(ns ziggurat.resource.dead-set
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]
            [ziggurat.config :refer [get-in-config]]
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

(defn- validate-params [count topic-entity stream-routes channel]
  (and (some? (get-in stream-routes [(keyword topic-entity) (or (keyword channel) :handler-fn)]))
       (validate-count count)))

(defn- not-found-handler-for-retry [_]
  {:status 404
   :body {:error "Retry is not enabled"}})

(defn retry-enabled? []
  (get-in-config [:retry :enabled]))

(defn get-replay []
  (if-not (retry-enabled?)
    not-found-handler-for-retry
    (let [stream-routes (:stream-routes (mount/args))]
      (fn [{{:keys [count topic-entity channel]} :params}]
        (let [parsed-count (parse-count count)]
          (if (validate-params parsed-count topic-entity stream-routes channel)
            (do (r/replay parsed-count topic-entity channel)
                {:status 200
                 :body   {:message "Requeued messages on the queue for retrying"}})
            {:status 400
             :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}}))))))

(defn get-view []
  (if-not (retry-enabled?)
    not-found-handler-for-retry
    (let [stream-routes (:stream-routes (mount/args))]
      (fn view [{{:keys [count topic-entity channel]} :params}]
        (let [parsed-count (parse-count count)]
          (if (validate-params parsed-count topic-entity stream-routes channel)
            {:status 200
             :body   {:messages (r/view parsed-count topic-entity channel)}}
            {:status 400
             :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}}))))))

(defn delete-messages []
  (if-not (retry-enabled?)
    not-found-handler-for-retry
    (let [stream-routes (:stream-routes (mount/args))]
      (fn [{{:keys [count topic-entity channel]} :params}]
        (let [parsed-count (parse-count count)]
          (if (validate-params parsed-count topic-entity stream-routes channel)
            (do
              (r/delete parsed-count topic-entity channel)
              {:status 200
               :body   {:message "Deleted messages successfully"}})
            {:status 400
             :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}}))))))