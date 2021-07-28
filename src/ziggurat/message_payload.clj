(ns ziggurat.message-payload
  (:require [schema.core :as s]))

(defrecord MessagePayload [message topic-entity])

(defn mk-message-payload
  [msg topic-entity metadata]
  {:message msg :topic-entity (name topic-entity) :metadata metadata})
