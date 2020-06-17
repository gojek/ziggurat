(ns ziggurat.server.middleware-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.server.middleware :refer [wrap-with-metrics wrap-swagger]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.metrics :as metrics]
            [ring.swagger.swagger-ui :as rsui]))

(deftest wrap-with-metrics-test
  (testing "should publish http-server metrics"
    (let [increment-count-called (atom false)
          request-uri            "/ping"
          request                {:uri request-uri}
          response-status        404
          response               {:status response-status}
          handler                (fn [request-arg] response)]
      (with-redefs [metrics/increment-count (fn [metrics-namespaces metrics additional-tags]
                                              (if (and
                                                   (is (= "http-server" (first metrics-namespaces)))
                                                   (is (= "requests-served" (second metrics-namespaces)))
                                                   (is (= "count" metrics))
                                                   (is (= request-uri (:request-uri additional-tags)))
                                                   (is (= (str response-status) (:response-status additional-tags))))
                                                (swap! increment-count-called not)))]
        ((wrap-with-metrics handler) request)
        (is (= @increment-count-called true))))))

(deftest wrap-swagger-test
  (testing "when swagger config is enabled"
    (let [wrap-swagger-ui-call-count (atom 0)
          handler (fn [_] {:status 200})
          original-zig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (assoc-in original-zig-config [:http-server :middlewares :swagger :enabled] true))
                    rsui/wrap-swagger-ui (fn [& _] (swap! wrap-swagger-ui-call-count inc))]
        (wrap-swagger handler)
        (is (= @wrap-swagger-ui-call-count 1)))))
  (testing "when swagger config is disabled"
    (let [wrap-swagger-ui-call-count (atom 0)
          handler (fn [_] {:status 200})
          original-zig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (assoc-in original-zig-config [:http-server :middlewares :swagger :enabled] false))
                    rsui/wrap-swagger-ui (fn [& _] (swap! wrap-swagger-ui-call-count inc))]
        (wrap-swagger handler)
        (is (= @wrap-swagger-ui-call-count 0))))))
