(ns ziggurat.message-payload
  (:require [schema.core :as s]))

(defrecord MessagePayload [message topic-entity])
