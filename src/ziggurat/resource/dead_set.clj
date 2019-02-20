(ns ziggurat.resource.dead-set
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]
            [ziggurat.config :refer [get-in-config channel-retry-config]]
            [ziggurat.messaging.dead-set :as r]
            [mount.core :as mount]))
(def not-found-for-retry
  {:status 404
   :body {:error "Retry is not enabled"}})

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

(defn- channel-request? [channel]
  (some? channel))

(defn retry-enabled? []
  (get-in-config [:config :enabled]))

(defn channel-retry-enabled? [topic-entity channel]
  (get-in (channel-retry-config (keyword topic-entity) (keyword channel)) [:enabled]))

(defn retry-allowed? [topic-entity channel]
  (if (channel-request? channel)
    (channel-retry-enabled? topic-entity channel)
    (retry-enabled?)))

(defn get-replay []
  (let [stream-routes (:stream-routes (mount/args))]
    (fn [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (retry-allowed? topic-entity channel)
          (if (validate-params parsed-count topic-entity stream-routes channel)
            (do (r/replay parsed-count topic-entity channel)
                {:status 200
                 :body   {:message "Requeued messages on the queue for retrying"}})
            {:status 400
             :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}})
          not-found-for-retry)))))

(defn get-view []
  (let [stream-routes (:stream-routes (mount/args))]
    (fn view [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (retry-allowed? topic-entity channel)
          (if (validate-params parsed-count topic-entity stream-routes channel)
            {:status 200
             :body   {:messages (r/view parsed-count topic-entity channel)}}
            {:status 400
             :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}})
          not-found-for-retry)))))

(defn delete-messages []
  (let [stream-routes (:stream-routes (mount/args))]
    (fn [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (retry-allowed? topic-entity channel)
          (if (validate-params parsed-count topic-entity stream-routes channel)
            (do
              (r/delete parsed-count topic-entity channel)
              {:status 200
               :body   {:message "Deleted messages successfully"}})
            {:status 400
             :body   {:error "Count should be the positive integer and topic entity/ channel should be present"}})
          not-found-for-retry)))))