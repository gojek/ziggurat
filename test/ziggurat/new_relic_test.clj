(ns ziggurat.new-relic-test
  (:require [clojure.test :refer :all]
            [ziggurat.new-relic :as nr]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix])
  (:import (com.newrelic.api.agent NewRelic)))

(use-fixtures :once fix/mount-only-config)

(deftest report-error-test
  (let [config (ziggurat-config)]
    (testing "When new-relic is enabled in the config, error is reported"
      (let [error-reported? (atom false)]
        (with-redefs [ziggurat-config (fn [] (assoc-in config [:new-relic :report-errors] true))
                      nr/notice-error (fn [_ _] (reset! error-reported? true))]
          (nr/report-error (Exception. "Error") nil)
          (is @error-reported?))))
    (testing "When new-relic is disabled in the config, error isn't reported"
      (let [error-reported? (atom false)]
        (with-redefs [ziggurat-config (fn [] (assoc-in config [:new-relic :report-errors] false))
                      nr/notice-error (fn [_ _] (reset! error-reported? true))]
          (nr/report-error (Exception. "Error") nil)
          (is (false? @error-reported?)))))))
