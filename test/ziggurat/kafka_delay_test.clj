(ns ziggurat.kafka-delay-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-delay :refer :all]
            [ziggurat.util.time :refer [get-current-time-in-millis]]
            [ziggurat.metrics :as metrics]))

(deftest calculate-and-report-kafka-delay-test
  (testing "calculates and reports the timestamp delay"
    (let [record-timestamp 1528720767777
          current-time     1528720768777
          expected-delay   1000
          namespace        "test"]
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time        (fn [metric-namespace delay]
                                                 (is (= delay expected-delay))
                                                 (is (= metric-namespace namespace)))]
        (calculate-and-report-kafka-delay namespace record-timestamp)))))

