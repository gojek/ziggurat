(ns ziggurat.ssl.properties
  (:require [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.ssl.constants :as const])
  (:import [org.apache.kafka.streams StreamsConfig]
           [org.apache.kafka.common.config SslConfigs SaslConfigs]))

(defn- ssl-configs
  [key]
  (get-in (ziggurat-config) [:ssl-config key]))

(defn- validate-mechanism
  [mechanism]
  (when-not (contains? #{const/mechanism-plain const/mechanism-scram-512} mechanism)
    (let [message (format "SSL mechanism can either be %s or %s" const/mechanism-plain const/mechanism-scram-512)]
      (throw (IllegalArgumentException. message {:mechanism mechanism})))))

(defn- validate-protocol
  [protocol]
  (when-not (contains? #{const/protocol-plaintext const/protocol_sasl_ssl} protocol)
    (let [message (format "protocol can either be %s or %s" const/protocol-plaintext const/protocol_sasl_ssl)]
      (throw (ex-info message {:protocol protocol})))))

(defn- create-jaas-properties
  [user-name password mechanism]
  (validate-mechanism mechanism)
  (let [jaas-template (const/get-jaas-template mechanism)]
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
      (do (validate-protocol protocol)
          (doto properties
            (.put SaslConfigs/SASL_JAAS_CONFIG (create-jaas-properties user-name password mechanism))
            (.put SaslConfigs/SASL_MECHANISM mechanism)
            (.put SslConfigs/SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG endpoint-algorithm-config)
            (.put StreamsConfig/SECURITY_PROTOCOL_CONFIG protocol)))
      properties)))