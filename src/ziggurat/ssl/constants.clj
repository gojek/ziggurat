(ns ziggurat.ssl.constants)

(def mechanism-plain "PLAIN")
(def mechanism-scram-512 "SCRAM-SHA-512")

(def protocol-plaintext "PLAINTEXT")
(def protocol_sasl_ssl "SASL_SSL")

(def jaas-template
  {mechanism-plain     "org.apache.kafka.common.security.plain.PlainLoginModule"
   mechanism-scram-512 "org.apache.kafka.common.security.scram.ScramLoginModule"})

(defn get-jaas-template
  [mechanism]
  (get jaas-template mechanism))
