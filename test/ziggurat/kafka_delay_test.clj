(ns ziggurat.kafka-delay-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-delay :refer :all]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.time :refer [get-current-time-in-millis]]))

(deftest calculate-and-report-kafka-delay-test
  (testing "calculates and reports the timestamp delay"
    (let [record-timestamp           1528720767777
          current-time               1528720768777
          expected-delay             1000
          namespace                  "test"
          expected-topic-entity-name "expected-topic-entity-name"]
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time        (fn [metric-namespace delay topic-entity-name]
                                                 (is (= delay expected-delay))
                                                 (is (= metric-namespace namespace))
                                                 (is (= topic-entity-name expected-topic-entity-name)))]
        (calculate-and-report-kafka-delay namespace record-timestamp expected-topic-entity-name)))))
