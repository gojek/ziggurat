(ns ziggurat.resource.dead-set-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.resource.dead-set :refer [get-view]]
            [ziggurat.config :as config]))

(deftest get-view-test
  (let [ziggurat-config (config/ziggurat-config)]
    (testing "returns 422 when retries are not enabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (is (= {:status 422, :body {:error "Retries are not enabled"}} (get-view)))))))