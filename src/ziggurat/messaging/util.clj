(ns ziggurat.messaging.util
  (:require [camel-snake-kebab.core :as csk]))

(defn prefixed-queue-name [topic-entity value]
  (csk/->snake_case_string (str (name topic-entity) "_" value)))
