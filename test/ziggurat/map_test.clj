(ns ziggurat.map-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.map :refer [deep-merge
                                  flatten-map-and-replace-defaults]]))


(deftest deep-merge-test
  (testing "when there is a value in map 'a' it returns value from 'a' "
    (let [a {:env "integration"}
          b {:env "test"}
          expected-map {:env "integration"}]
      (is (= (deep-merge a b) expected-map))))

  (testing "when a map is nil, take configuration from map b"
    (let [a {:env nil}
          b {:env "integration"}
          expected-map {:env "integration"}]
      (is (= (deep-merge a b) expected-map))))

  (testing "when there is a value in nested map 'a' it returns value from 'a'"
    (let [a {:endpoint {:host "localhost" :port "3000"}}
          b {:endpoint {:host "integration" :port "3001"}}
          expected-map {:endpoint {:host "localhost" :port "3000"}}]
      (is (= (deep-merge a b) expected-map)))))

(deftest flatten-map-and-replace-defaults-test
  (testing "when new-conf has values, return configuration from new-conf"
    (let [default-conf {:env     "integration"
                        :service {:host "http://localhost"
                                  :port [4000 :int]}
                        :enabled [false :bool]
                        :val     [200000 :double]}
          new-conf {"ENV"          "production"
                    "SERVICE_HOST" "http://integration.com"
                    "SERVICE_PORT" "5000"
                    "ENABLED"      "true"
                    "VAL"          "1323.5"}
          expected-conf {:env     "production"
                         :service {:host "http://integration.com"
                                   :port 5000}
                         :enabled true
                         :val     1323.5}]
      (is (= (flatten-map-and-replace-defaults default-conf new-conf) expected-conf))))

  (testing "when new-conf is nil, return nil"
    (let [default-conf {:env     "integration"
                        :service {:host "http://localhost"
                                  :port [4000 :int]}
                        :enabled [false :bool]
                        :val     [200000 :double]}
          new-conf {"ENV"          nil
                    "SERVICE_HOST" "http://integration.com"
                    "SERVICE_PORT" nil
                    "ENABLED"      "true"
                    "VAL"          "1323.5"}
          expected-conf {:env     nil
                         :service {:host "http://integration.com"
                                   :port nil}
                         :enabled true
                         :val     1323.5}]
      (is (= (flatten-map-and-replace-defaults default-conf new-conf) expected-conf)))))
