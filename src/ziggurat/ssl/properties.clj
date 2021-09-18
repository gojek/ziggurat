(ns ziggurat.ssl.properties
  (:require [ziggurat.config :refer [ziggurat-config]])
  (:import [org.apache.kafka.streams StreamsConfig]
           [org.apache.kafka.common.config SslConfigs SaslConfigs]))

(def jaas-template
  {"PLAIN"         "org.apache.kafka.common.security.plain.PlainLoginModule"
   "SCRAM-SHA-512" "org.apache.kafka.common.security.scram.ScramLoginModule"})

(defn- get-jaas-template
  [mechanism]
  (get jaas-template mechanism))

(defn- ssl-configs
  [key]
  (get-in (ziggurat-config) [:ssl-config key]))

(defn- create-jaas-properties
  [user-name password mechanism]
  (let [jaas-template (get-jaas-template mechanism)]
    (format "%s required username=\"%s\" password=\"%s\";" jaas-template user-name password)))

(defn build-ssl-properties
  [properties]
  (let [enabled (ssl-configs :enabled)
        user-name (ssl-configs :user-name)
        password (ssl-configs :password)
        protocol (ssl-configs :protocol)
        mechanism (ssl-configs :mechanism)
        endpoint-algorithm-config (ssl-configs :endpoint-algorithm-config)]
    (if enabled
      (doto properties
        (.put SaslConfigs/SASL_JAAS_CONFIG (create-jaas-properties user-name password mechanism))
        (.put SaslConfigs/SASL_MECHANISM mechanism)
        (.put SslConfigs/SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG endpoint-algorithm-config)
        (.put StreamsConfig/SECURITY_PROTOCOL_CONFIG protocol))
      properties)))