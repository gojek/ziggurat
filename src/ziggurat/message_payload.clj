(ns ziggurat.message-payload
  (:require [schema.core :as s]))

(defrecord MessagePayload [message topic-entity])

(declare message-payload-schema)

(s/defschema message-payload-schema
             {:message                      s/Any
              :topic-entity                 s/Keyword
              (s/optional-key :retry-count) s/Int
              (s/optional-key :headers)     s/Any})
