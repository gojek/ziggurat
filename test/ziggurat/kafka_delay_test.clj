(ns ziggurat.kafka-delay-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-delay :refer :all]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.time :refer [get-current-time-in-millis]]))

(deftest calculate-and-report-kafka-delay-test
  (let [record-timestamp    1528720767777
        current-time        1528720768777
        expected-delay      1000
        expected-namespaces ["test"]]
    (testing "calculates and reports the timestamp delay"
      (let [expected-additional-tags {:topic-name "expected-topic-entity-name"}]
        (with-redefs [get-current-time-in-millis (constantly current-time)
                      metrics/report-time        (fn [metric-namespaces delay additional-tags]
                                                   (is (= delay expected-delay))
                                                   (is (= metric-namespaces expected-namespaces))
                                                   (is (= additional-tags expected-additional-tags)))]
          (calculate-and-report-kafka-delay expected-namespaces record-timestamp expected-additional-tags))))
    (testing "calculates and reports the timestamp delay when additional tags is empty or nil"
      (let [expected-additional-tags nil]
        (with-redefs [get-current-time-in-millis (constantly current-time)
                      metrics/report-time        (fn [metric-namespaces delay additional-tags]
                                                   (is (= delay expected-delay))
                                                   (is (= metric-namespaces expected-namespaces))
                                                   (is (= additional-tags expected-additional-tags)))]
          (calculate-and-report-kafka-delay expected-namespaces record-timestamp))))))
