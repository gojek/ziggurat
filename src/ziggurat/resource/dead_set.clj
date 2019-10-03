(ns ziggurat.resource.dead-set
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount]
            [schema.core :as s]
            [ziggurat.config :refer [get-in-config channel-retry-config]]
            [ziggurat.messaging.dead-set :as r]))
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
  (let [channel-val (get-in stream-routes [(keyword topic-entity) (keyword channel)])
        ;;TODO: Remove the usage of :handler-fn when it is deprecated
        handler-fn-val (get-in stream-routes [(keyword topic-entity) :handler-fn])
        handler-val (get-in stream-routes [(keyword topic-entity) :handler])]
  (and (or (some? channel-val) (some? handler-fn-val) (some? handler-val))
       (validate-count count))))

(defn- channel-request? [channel]
  (some? channel))

(defn retry-enabled? []
  (get-in-config [:retry :enabled]))

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