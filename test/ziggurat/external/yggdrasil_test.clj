(ns ziggurat.external.yggdrasil-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.external.yggdrasil :refer [get-config]]
            [clj-http.client :as http]))



(deftest get-config-test
  (let [env       "production"
        port      3100
        app-name "foo"]
    (testing "when yggdrasil responds with correct config"
      (with-redefs [http/get (fn [url attr]
                               (is (= (:query-params attr) {"q" env}))
                               (is (= "http://localhost:3100/v1/configurations/foo/latest" url))
                               {:status 200,
                                :body   "{\"success\": true, \"data\":{\"k1\":\"v3\",\"k3\":false}}"})]
        (is (= {"k1" "v3", "k3" false} (get-config app-name "http://localhost" port env 10000)))))

    (testing "when yggdrasil responds with 404 error code"
      (with-redefs [http/get (fn [url attr]
                               (is (= (:query-params attr) {"q" env}))
                               (is (= "http://localhost:3100/v1/configurations/app_not_present/latest" url))
                               {:status 404,
                                :body   nil})]
        (is (nil? (get-config "app_not_present" "http://localhost" port env 10000)))))

    (testing "when yggdrasil responds with 500 error code"
      (with-redefs [http/get (fn [url attr]
                               (is (= (:query-params attr) {"q" env}))
                               (is (= "http://localhost:3100/v1/configurations/foo/latest" url))
                               {:status 500,
                                :body   nil})]
        (is (nil? (get-config app-name "http://localhost" port env 10000)))))
    ))