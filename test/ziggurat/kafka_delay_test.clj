(ns ziggurat.kafka-delay-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-delay :refer :all]
            [lambda-common.metrics :as metrics]))

(deftest calculate-and-report-kafka-delay-test
  (testing ""
    (let [record-timestamp 1528720767777
          milli-seconds 1528720768777
          expected-delay 1000
          namespace "test"]
      (with-redefs [get-current-time-in-millis (constantly milli-seconds)
                    metrics/report-time (fn [metric-namespace delay]
                                          (is (= delay expected-delay))
                                          (is (= metric-namespace namespace)))]
        (calculate-and-report-kafka-delay namespace record-timestamp)))))
