(ns ziggurat.middleware.default-test
  (:require [clojure.test :refer :all]
            [sentry-clj.async :as sentry]
            [protobuf.core :as proto]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]
            [ziggurat.middleware.default :refer :all :as mw])
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
          proto-message      (proto/->bytes (proto/create Example$Photo message))
          handler-fn         (fn [msg]
                               (if (= msg message)
                                 (reset! handler-fn-called? true)))]
      ((protobuf->hash handler-fn proto-class topic-entity-name) proto-message)
      (is (true? @handler-fn-called?))))
  (testing "When deserialisation fails, it reports to sentry, publishes metrics and passes nil to handler function"
    (let [handler-fn-called?      (atom false)
          metric-reporter-called? (atom false)
          topic-entity-name       "test"
          handler-fn              (fn [msg]
                                    (if (nil? msg)
                                      (reset! handler-fn-called? true)))]
      (with-redefs [metrics/multi-ns-increment-count (fn [_ _ _]
                                                       (reset! metric-reporter-called? true))]
        ((protobuf->hash handler-fn nil topic-entity-name) nil))
      (is (true? @handler-fn-called?))
      (is (true? @metric-reporter-called?))))
  (testing "When an already deserialised message is passed to the function it calls the handler fn without altering it"
    (let [handler-fn-called? (atom false)
          message            {:id   7
                              :path "/photos/h2k3j4h9h23"}
          proto-class        Example$Photo
          topic-entity-name  "test"
          handler-fn         (fn [msg]
                               (if (= msg message)
                                 (reset! handler-fn-called? true)))]
      ((protobuf->hash handler-fn proto-class topic-entity-name) message)
      (is (true? @handler-fn-called?))))
  (testing "When alpha feature is enabled use the new deserializer function"
    (let [deserialise-message-called?            (atom false)
          deserialise-message-deprecated-called? (atom false)
          topic-entity-name                      "test"]
      (with-redefs [mw/deserialise-message            (fn [_ _ _] (reset! deserialise-message-called? true))
                    ziggurat.config/ziggurat-config   (fn [] {:alpha-features {:protobuf-middleware {:enabled true}}})
                    mw/deserialise-message-deprecated (fn [_ _ _] (reset! deserialise-message-deprecated-called? true))]
        ((protobuf->hash (constantly nil) Example$Photo topic-entity-name) nil)
        (is (true? @deserialise-message-called?))
        (is (false? @deserialise-message-deprecated-called?)))))
  (testing "When alpha feature is disabled use the old deserializer function"
    (let [deserialise-message-called?            (atom false)
          deserialise-message-deprecated-called? (atom false)
          topic-entity-name                      "test"]
      (with-redefs [mw/deserialise-message            (fn [_ _ _] (reset! deserialise-message-called? true))
                    ziggurat.config/ziggurat-config   (fn [] {:alpha-features {:protobuf-middleware {:enabled false}}})
                    mw/deserialise-message-deprecated (fn [_ _ _] (reset! deserialise-message-deprecated-called? true))]
        ((protobuf->hash (constantly nil) Example$Photo topic-entity-name) nil)
        (is (true? @deserialise-message-deprecated-called?))
        (is (false? @deserialise-message-called?))))))

(deftest protobuf->hash-test-deprecated
  (with-redefs [ziggurat.config/ziggurat-config (fn [] {:alpha-features {:protobuf-middleware {:enabled false}}})]
    (protobuf->hash-test)))
