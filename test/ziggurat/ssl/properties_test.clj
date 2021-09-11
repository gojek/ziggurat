(ns ziggurat.ssl.properties-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.ssl.properties :refer [build-ssl-properties]])
  (:import [java.util Properties]
           [org.apache.kafka.streams StreamsConfig]
           [org.apache.kafka.common.config SslConfigs SaslConfigs]))

(use-fixtures :once fix/mount-only-config)

(deftest build-ssl-properties-test
  (let [config (ziggurat-config)
        properties (Properties.)]
    (testing "should return properties when ziggurat.ssl-config is not present"
      (is (= properties (build-ssl-properties properties))))

    (testing "should return properties when ziggurat.ssl-config is not enabled"
      (let [ssl-configs {:enabled                   true
                         :user-name                 "test-user"
                         :password                  "test-password"
                         :protocol                  "SASL_SSL"
                         :mechanism                 "SCRAM-SHA-512"
                         :endpoint-algorithm-config "https"}]
        (with-redefs [ziggurat-config (fn [] (assoc-in config [:ssl-config] ssl-configs))]
          (is (= properties (build-ssl-properties properties))))))

    (testing "should throw exception when ziggurat.ssl protocol is not supported"
      (let [ssl-configs {:enabled                   true
                         :user-name                 "test-user"
                         :password                  "test-password"
                         :protocol                  "INVALID_PROTOCOL"
                         :mechanism                 "SCRAM-SHA-512"
                         :endpoint-algorithm-config "https"}
            exception-message #"protocol can either be PLAINTEXT or SASL_SSL"
            exception-cause {:protocol "INVALID_PROTOCOL"}]
        (with-redefs [ziggurat-config (fn [] (assoc-in config [:ssl-config] ssl-configs))]
          (is (thrown? Exception exception-message exception-cause (build-ssl-properties properties))))))

    (testing "should throw exception when ziggurat.ssl mechanism is not supported"
      (let [ssl-configs {:enabled                   true
                         :user-name                 "test-user"
                         :password                  "test-password"
                         :protocol                  "SASL_SSL"
                         :mechanism                 "INVALID_MECHANISM"
                         :endpoint-algorithm-config "https"}
            exception-message #"SSL mechanism can either be PLAIN or SCRAM-SHA-512"
            exception-cause {:mechanism "INVALID_MECHANISM"}]
        (with-redefs [ziggurat-config (fn [] (assoc-in config [:ssl-config] ssl-configs))]
          (is (thrown? Exception exception-message exception-cause (build-ssl-properties properties))))))

    (testing "should build and return properties with SSL configs"
      (let [ssl-configs {:enabled                   true
                         :user-name                 "test-user"
                         :password                  "test-password"
                         :protocol                  "SASL_SSL"
                         :mechanism                 "SCRAM-SHA-512"
                         :endpoint-algorithm-config "https"}
            expected-properties (doto properties
                                  (.put SaslConfigs/SASL_JAAS_CONFIG "org.apache.kafka.common.security.scram.ScramLoginModule required username=test-user password=test-password")
                                  (.put SaslConfigs/SASL_MECHANISM "SCRAM-SHA-512")
                                  (.put SslConfigs/SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG "https")
                                  (.put StreamsConfig/SECURITY_PROTOCOL_CONFIG "SASL_SSL"))]
        (with-redefs [ziggurat-config (fn [] (assoc-in config [:ssl-config] ssl-configs))]
          (is (= expected-properties (build-ssl-properties properties))))))))
