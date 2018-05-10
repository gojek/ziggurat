(ns ziggurat.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clonfig.core :as clonfig]
            [mount.core :refer [defstate]]
            [ziggurat.map :as umap]
            [ziggurat.external.yggdrasil :as yggdrasil]))

(def config-file "config.edn")

(defn edn-config [config-file]
  (-> config-file
      (io/resource)
      (slurp)
      (edn/read-string)))

(defn config-from-env [config-file]
  (clonfig/read-config (edn-config config-file)))

(defn make-config [config-file]
  (let [edn-conf (edn-config config-file)
        env-config (config-from-env config-file)
        ziggurat-config (:ziggurat env-config)
        {:keys [host port connection-timeout-in-ms]} (:yggdrasil ziggurat-configg)
        env (:env ziggurat-config)
        app-name (:app-name ziggurat-config)
        yggdrasil-raw-config (yggdrasil/get-config app-name host port env connection-timeout-in-ms)]
    (if (nil? yggdrasil-raw-config)
      env-config
      (umap/deep-merge (umap/flatten-map-and-replace-defaults edn-conf
                                                              yggdrasil-raw-config)
                       env-config))))

(defstate config
  :start (make-config config-file))

(defn ziggurat-config []
  (get config :ziggurat))

(defn rabbitmq-config []
  (:rabbit-mq (ziggurat-config)))