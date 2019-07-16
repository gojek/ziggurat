(ns ziggurat.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clonfig.core :as clonfig]
            [mount.core :refer [defstate]])
  (:gen-class
    :name tech.gojek.ziggurat.Config
    :methods [^{:static true} [get [clojure.lang.Keyword] Object]
              ^{:static true} [getIn [clojure.lang.PersistentVector] Object]]))


(def config-file "config.edn")

(def default-config {:ziggurat {:nrepl-server         {:port 70171}
                                :datadog              {:port    8125
                                                       :enabled false}
                                :sentry               {:enabled                   false
                                                       :worker-count              10
                                                       :queue-size                10
                                                       :thread-termination-wait-s 1}
                                :rabbit-mq-connection {:port            5672
                                                       :username        "guest"
                                                       :password        "guest"
                                                       :channel-timeout 2000}
                                :jobs                 {:instant {:worker-count   4
                                                                 :prefetch-count 4}}
                                :rabbit-mq            {:delay       {:queue-name           "%s_delay_queue"
                                                                     :exchange-name        "%s_delay_exchange"
                                                                     :dead-letter-exchange "%s_instant_exchange"
                                                                     :queue-timeout-ms     5000}
                                                       :instant     {:queue-name    "%s_instant_queue"
                                                                     :exchange-name "%s_instant_exchange"}
                                                       :dead-letter {:queue-name    "%s_dead_letter_queue"
                                                                     :exchange-name "%s_dead_letter_exchange"}}
                                :retry                {:count   5
                                                       :enabled false}
                                :http-server          {:port         8080
                                                       :thread-count 100}}})

(defn- interpolate-val [val app-name]
  (if (string? val)
    (format val app-name)
    val))

(defn- interpolate-config [config app-name]
  (reduce-kv (fn [m k v]
               (if (map? v)
                 (assoc m k (interpolate-config v app-name))
                 (assoc m k (interpolate-val v app-name)))) {} config))

(defn- deep-merge [& maps]
  (apply merge-with (fn [& args]
                      (if (every? map? args)
                        (apply deep-merge args)
                        (last args)))
         maps))

(defn- edn-config [config-file]
  (-> config-file
      (io/resource)
      (slurp)
      (edn/read-string)))

(defn config-from-env [config-file]
  (clonfig/read-config (edn-config config-file)))

(defstate config
  :start (let [config-values-from-env (config-from-env config-file)
               app-name (-> config-values-from-env :ziggurat :app-name)]
           (deep-merge (interpolate-config default-config app-name) config-values-from-env)))

(defn ziggurat-config []
  (get config :ziggurat))

(defn rabbitmq-config []
  (get (ziggurat-config) :rabbit-mq))

(defn get-in-config [ks]
  (get-in (ziggurat-config) ks))

(defn channel-retry-config [topic-entity channel]
  (get-in (ziggurat-config) [:stream-router topic-entity :channels channel :retry]))


(defn -getIn [keys]
  (get-in config keys))

(defn -get [key]
   (-getIn [key]))

