(ns ziggurat.middleware.default-test
  (:require [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [protobuf.core :as proto]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]
            [ziggurat.middleware.default :as mw])
  (:import [flatland.protobuf.test Example$Photo]
           [ziggurat.middleware.default RegularMessage StreamJoinsMessage]))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

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
      ((mw/protobuf->hash handler-fn proto-class topic-entity-name) (mw/->RegularMessage proto-message))
      (is (true? @handler-fn-called?))))
  (testing "deserialize a message from a stream join"
    (let [handler-fn-called?  (atom false)
          left-message        {:id   123
                               :path "/path/to/left"}
          right-message       {:id   456
                               :path "/path/to/right"}
          proto-class         Example$Photo
          topic-entity-name   "test"
          left-proto-message  (proto/->bytes (proto/create Example$Photo left-message))
          right-proto-message (proto/->bytes (proto/create Example$Photo right-message))
          handler-fn          (fn [{:keys [left right]}]
                                (if (and (= left left-message)
                                         (= right right-message))
                                  (reset! handler-fn-called? true)))]
      ((mw/protobuf->hash handler-fn proto-class topic-entity-name) (mw/->StreamJoinsMessage {:left left-proto-message :right right-proto-message}))
      (is (true? @handler-fn-called?))))
  (testing "deserialize a message from a stream join using 2 proto classes"
    (let [handler-fn-called?  (atom false)
          left-message        {:id   123
                               :path "/path/to/left"}
          right-message       {:id   456
                               :path "/path/to/right"}
          proto-class         Example$Photo
          topic-entity-name   "test"
          left-proto-message  (proto/->bytes (proto/create Example$Photo left-message))
          right-proto-message (proto/->bytes (proto/create Example$Photo right-message))
          handler-fn          (fn [{:keys [left right]}]
                                (if (and (= left left-message)
                                         (= right right-message))
                                  (reset! handler-fn-called? true)))]
      ((mw/protobuf->hash handler-fn [proto-class proto-class] topic-entity-name) (mw/->StreamJoinsMessage {:left left-proto-message :right right-proto-message}))
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
      ((mw/protobuf->hash handler-fn proto-class topic-entity-name) (mw/->RegularMessage message))
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
        ((mw/protobuf->hash handler-fn nil topic-entity-name) (mw/->RegularMessage nil)))
      (is (true? @handler-fn-called?))
      (is (true? @metric-reporter-called?)))))

(deftest protobuf->hash-test-alpha-and-deprecated
  (testing "Deprecated protobuf deserializer"
    (common-protobuf->hash-test)
    (testing "When alpha feature is disabled use the old deserializer function"
      (let [deserialize-message-called? (atom false)
            topic-entity-name           "test"
            message                     {:id   7
                                         :path "/photos/h2k3j4h9h23"}
            proto-class                 Example$Photo
            proto-message               (proto/->bytes (proto/create Example$Photo message))]
        (with-redefs [mw/deserialize-message (fn [_ _ _] (reset! deserialize-message-called? true))]
          ((mw/protobuf->hash (constantly nil) proto-class topic-entity-name) (mw/->RegularMessage proto-message))
          (is (true? @deserialize-message-called?))))))
  (testing "Alpha protobuf deserializer"
    (common-protobuf->hash-test)
    (testing "When alpha feature is enabled use the new deserializer function"
      (let [deserialize-message-called? (atom false)
            topic-entity-name           "test"
            message                     {:id   7
                                         :path "/photos/h2k3j4h9h23"}
            proto-class                 Example$Photo
            proto-message               (proto/->bytes (proto/create Example$Photo message))]
        (with-redefs [mw/deserialize-message (fn [_ _ _] (reset! deserialize-message-called? true))]

          ((mw/protobuf->hash (constantly nil) proto-class topic-entity-name) (mw/->RegularMessage proto-message))
          (is (true? @deserialize-message-called?)))))))
