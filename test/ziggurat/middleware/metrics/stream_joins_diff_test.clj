(ns ziggurat.middleware.metrics.stream-joins-diff-test
  (:require [clojure.test :refer :all]
            [ziggurat.middleware.metrics.stream-joins-diff :as mw]
            [ziggurat.metrics :as metrics]
            [ziggurat.fixtures :as fix]))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(deftest stream-joins-diff-test
  (testing "this middleware publishes the metrics and then calls the provided handler function"
    (let [handler-fn-called?     (atom false)
          metrics-published?      (atom false)
          message                {:topic-1 {:event-timestamp {:nanos 123}} :topic-2 {:event-timestamp {:nanos 456}}}
          handler-fn             (fn [msg]
                                   (if (and (true? @metrics-published?) (= msg message))
                                     (reset! handler-fn-called? true)))]
      (with-redefs [metrics/report-histogram (fn [metric-namespaces val tags]
                                               (if (and (= "application_name" (first metric-namespaces))
                                                        (= "stream-joins-message-diff" (second metric-namespaces))
                                                        (= val (- 456 123))
                                                        (= tags {:left "topic-1" :right "topic-2"})
                                                        (false? @metrics-published?))
                                                 (reset! metrics-published? true)))]
        ((mw/publish-diff-between-joined-messages handler-fn) message)
        (is (true? @handler-fn-called?))))))

