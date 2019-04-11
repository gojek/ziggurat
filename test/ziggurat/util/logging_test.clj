(ns ziggurat.util.logging-test
  (:require [clojure.test :refer :all]
            [ziggurat.util.logging :as zlog])
  (:import (org.apache.logging.log4j ThreadContext)))

(deftest with-context-test
  (let [capture-context #(reset! % (into {} (ThreadContext/getImmutableContext)))]
    (testing "sets thread local log context params within body"
      (let [context-map (atom {})]
        (zlog/with-context {:param-1 "string-value", :param-2 123}
          (capture-context context-map))
        (is (= "string-value" (get @context-map "param-1")))
        (is (= "123" (get @context-map "param-2")))))

    (testing "clears thread local log context params when exits"
      (let [context-map (atom {})]
        (zlog/with-context {:param-1 "string-value", :param-2 123}
          (constantly nil))
        (capture-context context-map)
        (is (empty? @context-map))))))
