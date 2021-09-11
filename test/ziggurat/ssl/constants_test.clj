(ns ziggurat.ssl.constants-test
  (:require [clojure.test :refer :all]
            [ziggurat.ssl.constants :as const]))

(deftest get-jaas-template-test
  (testing "should return jaas plain template when mechanism is PLAIN"
    (is (= "org.apache.kafka.common.security.plain.PlainLoginModule" (const/get-jaas-template const/mechanism-plain))))

  (testing "should return jaas scram 512 template when mechanism is SCRAM-SHA-512"
    (is (= "org.apache.kafka.common.security.scram.ScramLoginModule" (const/get-jaas-template const/mechanism-scram-512)))))
