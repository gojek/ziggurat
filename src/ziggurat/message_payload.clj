(ns ziggurat.message-payload
  (:require [schema.core :as s]
            [ziggurat.config :refer [retry-count]]))

(defrecord MessagePayload [message topic-entity])

(defn mk-message-payload
  ([msg topic-entity]
   (mk-message-payload msg topic-entity (retry-count)))
  ([msg topic-entity retry-count]
   {:message msg :topic-entity (name topic-entity) :retry-count retry-count}))



