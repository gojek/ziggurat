(ns ziggurat.server.middleware-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.server.middleware :refer [publish-metrics]]
            [ziggurat.metrics :as metrics]))

(deftest publish-metrics-test
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
        ((publish-metrics handler) request)
        (is (= @increment-count-called true))))))
