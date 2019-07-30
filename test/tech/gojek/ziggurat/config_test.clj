(ns tech.gojek.ziggurat.config-test
  (:require [clojure.test :refer :all])
  (:import (tech.gojek.ziggurat Config)
           (tech.gojek.ziggurat.test Fixtures)))

(defn- ^java.util.List create-java-list []
  (doto (new java.util.ArrayList)
    (.add "ziggurat")
    (.add "app-name")))

(defn- create-java-array-of-strings []
  (into-array String (create-java-list)))

(defn- mount-config-using-java-class [f]
  (Fixtures/mountConfig)
  (f)
  (Fixtures/unmountAll))

(use-fixtures :once mount-config-using-java-class)

(deftest should-return-value-from-config
  (testing "Given a key, should return a valid value from Ziggurat config"
    (is (map? (Config/get "ziggurat")))))

(deftest should-return-inner-value-from-a-list-of-keys
  (testing "Given a Java list of keys, should return a valid nested value from Ziggurat config"
    (is (= "application_name" (Config/getIn (create-java-list))))))

(deftest should-return-inner-value-from-an-array-of-keys
  (testing "Given a Java array of keys, should return a valid nested value from Ziggurat config"
    (is (= "application_name" (Config/getIn (create-java-array-of-strings))))))


