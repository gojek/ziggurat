(ns ziggurat.config
  (:require [camel-snake-kebab.core :as csk]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clonfig.core :as clonfig]
            [mount.core :refer [defstate]]))

(def config-file "config.edn")

(defn make-config [config-file]
  (-> config-file
      (io/resource)
      (slurp)
      (edn/read-string)
      (clonfig/read-config)))

(defstate config
  :start (make-config config-file))

(defn ziggurat-config []
  (get config :ziggurat))

(defn rabbitmq-config []
  (:rabbit-mq (ziggurat-config)))