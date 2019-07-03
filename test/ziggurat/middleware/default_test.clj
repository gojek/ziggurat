(ns ziggurat.middleware.default-test
  (:require [clojure.test :refer :all]
            [flatland.protobuf.core :as proto]
            [ziggurat.middleware.default :refer :all]
            [ziggurat.fixtures :as fix]
            [sentry-clj.async :as sentry]
            [ziggurat.metrics :as metrics])
  (:import (flatland.protobuf.test Example$Photo)))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(deftest protobuf->hash-test
  (testing "Given a serialised object and corresponding proto-class it deserialises the object into a clojure map and calls the handler-fn with that message"
    (let [handler-fn-called? (atom false)
          message            {:id   7
                              :path "/photos/h2k3j4h9h23"}
          proto-class        Example$Photo
          topic-entity-name  "test"
          proto-message      (proto/protobuf-dump (proto/protodef Example$Photo) message)
          handler-fn         (fn [msg]
                               (if (= msg message)
                                 (reset! handler-fn-called? true)))]
      ((protobuf->hash handler-fn proto-class topic-entity-name) proto-message)
      (is (true? @handler-fn-called?))))
  (testing "When deserialisation fails, it reports to sentry and publishes metrics"
    (let [handler-fn-called? (atom false)
          metric-reporter-called? (atom false)
          topic-entity-name "test"
          handler-fn (fn [msg]
                       (reset! handler-fn-called? true))]
      (with-redefs [metrics/multi-ns-increment-count (fn [_ _ _]
                                                       (reset! metric-reporter-called? true))]
        ((protobuf->hash handler-fn nil topic-entity-name) nil))
      (is (false? @handler-fn-called?))
      (is (true? @metric-reporter-called?)))))
