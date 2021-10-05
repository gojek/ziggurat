(ns ziggurat.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clonfig.core :as clonfig]
            [mount.core :refer [defstate]]
            [ziggurat.util.java-util :as util])
  (:import (java.util Properties)
           [org.apache.kafka.common.config SaslConfigs])
  (:gen-class
   :methods [^{:static true} [get [String] Object]
             ^{:static true} [getIn [java.lang.Iterable] Object]]
   :name tech.gojek.ziggurat.internal.Config))

(def config-file "config.edn")

(def default-config {:ziggurat {:nrepl-server         {:port 70171}
                                :statsd               {:port    8125
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
                                :http-server          {:middlewares  {:swagger {:enabled false}}
                                                       :port         8080
                                                       :thread-count 100}
                                :new-relic            {:report-errors false}
                                :log-format           "text"}})

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

(declare config)

(defstate config
  :start (let [config-values-from-env (config-from-env config-file)
               app-name               (-> config-values-from-env :ziggurat :app-name)]
           (deep-merge (interpolate-config default-config app-name) config-values-from-env)))

(defn ziggurat-config []
  (get config :ziggurat))

(defn ssl-config []
  (get-in config [:ziggurat :ssl]))

(defn rabbitmq-config []
  (get (ziggurat-config) :rabbit-mq))

(defn statsd-config []
  (let [cfg (ziggurat-config)]
    (get cfg :statsd)))

(defn get-in-config
  ([ks]
   (get-in (ziggurat-config) ks))
  ([ks default]
   (get-in (ziggurat-config) ks default)))

(defn channel-retry-config [topic-entity channel]
  (get-in (ziggurat-config) [:stream-router topic-entity :channels channel :retry]))

(defn- java-response
  "When returning config from -get or -getIn, we can either return a Map or string (based on the key/keys passed).
  Since we do not want to pass a ClojureMap to a Java application, we check whether the config-vals (config to be returned)
  is a string or a PersistentHashMap. If it is a PersistentHashMap, we convert it to a Java Map and then return it."
  [config-vals]
  (if (map? config-vals)
    (util/clojure->java-map config-vals)
    config-vals))

(defn -getIn [^java.lang.Iterable keys]
  (let [config-vals (get-in config (util/list-of-keywords keys))]
    (java-response config-vals)))

(defn -get [^String key]
  (let [config-vals (get config (keyword key))]
    (java-response config-vals)))

(def consumer-config-mapping-table
  {:auto-offset-reset-config        :auto-offset-reset
   :commit-interval-ms              :auto-commit-interval-ms
   :consumer-group-id               :group-id
   :default-api-timeout-ms-config   :default-api-timeout-ms
   :key-deserializer-class-config   :key-deserializer
   :session-timeout-ms-config       :session-timeout-ms
   :value-deserializer-class-config :value-deserializer})

(def producer-config-mapping-table
  {:key-serializer-class   :key-serializer
   :retries-config         :retries
   :value-serializer-class :value-serializer})

(def streams-config-mapping-table
  {:auto-offset-reset-config           :auto-offset-reset
   :default-api-timeout-ms-config      :default-api-timeout-ms
   :changelog-topic-replication-factor :replication-factor
   :session-timeout-ms-config          :session-timeout-ms
   :stream-threads-count               :num-stream-threads})

(def ^:private non-kafka-config-keys
  [:channels
   :consumer-type
   :input-topics
   :join-cfg
   :oldest-processed-message-in-s
   :origin-topic
   :poll-timeout-ms-config
   :producer
   :thread-count
   :enabled
   :jaas])

(defn- to-list
  [s]
  (if (empty? s)
    (list)
    (list s)))

(defn- to-string-key
  [mapping-table k]
  (-> (get mapping-table k k)
      (name)
      (str/replace #"-" ".")))

(defn- normalize-value
  [v]
  (str/trim
   (cond
     (keyword? v) (name v)
     :else        (str v))))

(defn set-property
  [mapping-table p k v]
  (when-not (some #(= k %) non-kafka-config-keys)
    (let [string-key (to-list (to-string-key mapping-table k))
          norm-value (to-list (normalize-value v))]
      (doseq [sk string-key
              nv norm-value]
        (.setProperty p sk nv))))
  p)

(def jaas-template
  {"PLAIN"         "org.apache.kafka.common.security.plain.PlainLoginModule"
   "SCRAM-SHA-512" "org.apache.kafka.common.security.scram.ScramLoginModule"})

(defn create-jaas-properties
  [user-name password mechanism]
  (let [jaas-template (get jaas-template mechanism)]
    (format "%s required username=\"%s\" password=\"%s\";" jaas-template user-name password)))

(defn- add-jaas-properties
  [properties jaas-config]
  (if (some? jaas-config)
    (let [username (get jaas-config :username)
          password (get jaas-config :password)
          mechanism (get jaas-config :mechanism)]
      (doto properties
        (.put SaslConfigs/SASL_JAAS_CONFIG (create-jaas-properties username password mechanism))))
    properties))

(defn build-ssl-properties
  [properties set-property-fn ssl-config-map]
  "Builds SSL properties from ssl-config-map which is a map where keys are
  Clojure keywords in kebab case. These keys are converted to Kafka properties by set-property-fn.

  SSL properties are set only if key sequence [:ziggurat :ssl :enabled] returns true.

  Creates JAAS template if values are provided in the map provided agains this key sequence
  [:ziggurat :ssl :jaas].

  Example of a ssl-config-map

  {:enabled true
   :ssl-keystore-location <>
   :ssl-keystore-password <>
    {:jaas {:username <>
            :password <>
            :mechanism <>}}}
  "
  (let [ssl-configs-enabled (:enabled ssl-config-map)
        jaas-config         (get ssl-config-map :jaas)]
    (if (true? ssl-configs-enabled)
      (as-> properties pr
            (add-jaas-properties pr jaas-config)
            (reduce-kv set-property-fn pr ssl-config-map))
      properties)))

(defn build-properties
  "Builds Properties object from the provided config-map which is a map where keys are
   Clojure keywords in kebab case. These keys are converted to Kafka properties by set-property-fn.

   First, ssl properties are set using the config map returned by a call to `ssl-config`

   Then, properties from the provided `config-map` are applied. `config-map` can carry properties for Streams,
   Consumer or Producer APIs

   The method allows individual Streams, Producer, Consumer APIs to override SSL configs
   if ssl properties are provided in `config-map`

   Example of a config-map for streams
    {:auto-offset-reset   :latest
     :replication-factor  2
     :group-id            \"foo\"}

   "
  [set-property-fn config-map]
  (as-> (Properties.) pr
        (build-ssl-properties pr set-property-fn (ssl-config))
        (reduce-kv set-property-fn pr config-map)))

(def build-consumer-config-properties (partial build-properties (partial set-property consumer-config-mapping-table)))

(def build-producer-config-properties (partial build-properties (partial set-property producer-config-mapping-table)))

(def build-streams-config-properties (partial build-properties (partial set-property streams-config-mapping-table)))
