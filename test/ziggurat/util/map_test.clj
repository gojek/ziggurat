(ns ziggurat.util.map-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.util.map :refer [deep-merge nested-map-keys]]))

(deftest deep-merge-test
  (testing "when there is a value in map 'a' it returns value from 'a' "
    (let [a {:oldest-processed-message-in-s 3600}
          b {:oldest-processed-message-in-s 3500}
          expected-map {:oldest-processed-message-in-s 3600}]
      (is (= (deep-merge a b) expected-map))))

  (testing "when a map is nil, take configuration from map b"
    (let [a {:oldest-processed-message-in-s nil}
          b {:oldest-processed-message-in-s 3500}
          expected-map {:oldest-processed-message-in-s 3500}]
      (is (= (deep-merge a b) expected-map))))

  (testing "when there is a value in nested map 'a' it returns value from 'a'"
    (let [a {:endpoint {:host "localhost" :port "3000"}}
          b {:endpoint {:host "integration" :port "3001"}}
          expected-map {:endpoint {:host "localhost" :port "3000"}}]
      (is (= (deep-merge a b) expected-map)))))

(deftest nested-map-keys-test
  (testing "applies a function on a key"
    (let [a {:something "value"}
          expected-result {"something" "value"}]
      (is (= (nested-map-keys name a) expected-result)))))
