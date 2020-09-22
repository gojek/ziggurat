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

(defn- validate-channel-or-topic-entity [topic-entity stream-routes channel]
  (some? (get-in stream-routes [(keyword topic-entity) (or (keyword channel) :handler-fn)])))

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

(defn- all-routes []
  (merge (:stream-routes (mount/args))
         (:batch-routes (mount/args))))

(defn get-replay []
  (let [routes (all-routes)]
    (fn [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (retry-allowed? topic-entity channel)
          (if (validate-channel-or-topic-entity topic-entity routes channel)
            (if (validate-count parsed-count)
              (do (r/replay parsed-count topic-entity channel)
                  {:status 200
                   :body   {:message "Requeued messages on the queue for retrying"}})
              {:status 400
               :body   {:error "Count should be positive integer"}})
            {:status 400
             :body   {:error "Topic entity/channel should be provided and must be present in stream routes"}})
          not-found-for-retry)))))

(defn get-view []
  (let [routes (all-routes)]
    (fn view [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (retry-allowed? topic-entity channel)
          (if (validate-channel-or-topic-entity topic-entity routes channel)
            (if (validate-count parsed-count)
              (do (r/view parsed-count topic-entity channel)
                  {:status 200
                   :body   {:messages (r/view parsed-count topic-entity channel)}})
              {:status 400
               :body   {:error "Count should be positive integer"}})
            {:status 400
             :body   {:error "Topic entity/channel should be provided and must be present in stream routes"}})
          not-found-for-retry)))))

(defn delete-messages []
  (let [routes (all-routes)]
    (fn [{{:keys [count topic-entity channel]} :params}]
      (let [parsed-count (parse-count count)]
        (if (retry-allowed? topic-entity channel)
          (if (validate-channel-or-topic-entity topic-entity routes channel)
            (if (validate-count parsed-count)
              (do (r/delete parsed-count topic-entity channel)
                  {:status 200
                   :body   {:message "Deleted messages successfully"}})
              {:status 400
               :body   {:error "Count should be positive integer"}})
            {:status 400
             :body   {:error "Topic entity/channel should be provided and must be present in stream routes"}})
          not-found-for-retry)))))