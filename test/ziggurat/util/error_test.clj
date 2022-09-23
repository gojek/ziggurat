(ns ziggurat.util.error-test
  (:require [clojure.test :refer :all]
            [sentry-clj.async :refer [sentry-report]]
            [ziggurat.new-relic :as nr]
            [ziggurat.util.error :refer [report-error]]))

(deftest report-error-test
  (testing "error gets reported to sentry and new-relic"
    (let [sentry-report-called? (atom false)
          newrelic-report-error-called? (atom false)]
      (with-redefs [nr/report-error (fn [_ _] (reset! newrelic-report-error-called? true))]
        (report-error (Exception. "Error") "error")
        (is (= false @sentry-report-called?))
        (is @newrelic-report-error-called?)))))
