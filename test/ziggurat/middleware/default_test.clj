(ns ziggurat.middleware.default-test
  (:require [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [protobuf.core :as proto]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]
            [ziggurat.middleware.default :as mw])
  (:import [flatland.protobuf.test Example$Photo]))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(deftest protobuf-struct->persistent-map-test
  (testing "should convert protobuf struct to persistent map"
    (is (= {:a ["1" "2" {:z false :x nil :c ["2" {:w 2.0 :e "r"}]} ["2" {:w 2.0 :e "r"}]]
            :b 2.0}
           (mw/protobuf-struct->persistent-map {:fields [{:key   "a"
                                                          :value {:list-value [{:string-value "1"}
                                                                               {:string-value "2"}
                                                                               {:struct-value {:fields
                                                                                               [{:key "z" :value {:bool-value false}}
                                                                                                {:key "x" :value {:null-value nil}}
                                                                                                {:key "c" :value {:list-value
                                                                                                                  [{:string-value "2"}
                                                                                                                   {:struct-value {:fields
                                                                                                                                   [{:key "w" :value {:number-value 2.0}}
                                                                                                                                    {:key "e" :value {:string-value "r"}}]}}]}}]}}
                                                                               {:list-value
                                                                                [{:string-value "2"}
                                                                                 {:struct-value {:fields
                                                                                                 [{:key "w" :value {:number-value 2.0}}
                                                                                                  {:key "e" :value {:string-value "r"}}]}}]}]}}
                                                         {:key   "b"
                                                          :value {:number-value 2.0}}]})))))

(deftest common-protobuf->hash-test
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
      ((mw/protobuf->hash handler-fn proto-class topic-entity-name) proto-message)
      (is (true? @handler-fn-called?))))
  (testing "When an already deserialised message is passed to the function it calls the handler fn without altering it"
    (let [handler-fn-called? (atom false)
          message            {:id   7
                              :path "/photos/h2k3j4h9h23"}
          proto-class        Example$Photo
          topic-entity-name  "test"
          handler-fn         (fn [msg]
                               (if (= msg message)
                                 (reset! handler-fn-called? true)))]
      ((mw/protobuf->hash handler-fn proto-class topic-entity-name) message)
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
        ((mw/protobuf->hash handler-fn nil topic-entity-name) nil))
      (is (true? @handler-fn-called?))
      (is (true? @metric-reporter-called?))))
  (testing "using the new deserializer function"
    (let [deserialize-message-called? (atom false)
          topic-entity-name           "test"
          message                     {:id   7
                                       :path "/photos/h2k3j4h9h23"}
          proto-class                 Example$Photo
          proto-message               (proto/->bytes (proto/create Example$Photo message))]
      (with-redefs [mw/deserialize-message (fn [_ _ _ _] (reset! deserialize-message-called? true))]
        ((mw/protobuf->hash (constantly nil) proto-class topic-entity-name) proto-message)
        (is (true? @deserialize-message-called?))))))
