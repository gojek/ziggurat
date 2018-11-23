(ns ziggurat.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clonfig.core :as clonfig]
            [mount.core :refer [defstate]]))

(def config-file "config.edn")

(defn edn-config [config-file]
  (-> config-file
      (io/resource)
      (slurp)
      (edn/read-string)))

(defn config-from-env [config-file]
  (clonfig/read-config (edn-config config-file)))

(defstate config
  :start (config-from-env config-file))

(defn ziggurat-config []
  (get config :ziggurat))

(defn rabbitmq-config []
  (get (ziggurat-config) :rabbit-mq))

(defn get-in-config [ks]
  (get-in (ziggurat-config) ks))
