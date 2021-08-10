(ns ziggurat.middleware.stream-joins-test
  (:require [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [protobuf.core :as proto]
            [ziggurat.fixtures :as fix]
            [ziggurat.middleware.stream-joins :as sjmw])
  (:import [flatland.protobuf.test Example$Photo]))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(deftest common-protobuf->hash-test
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
          handler-fn          (fn [{:keys [message metadata]}]
                                (when (and (= (:left message) left-message)
                                           (= (:right message) right-message)
                                           (= (:topic metadata) "topic")
                                           (= (:timestamp metadata) 1234567890)
                                           (= (:partition metadata) 1))
                                  (reset! handler-fn-called? true)))]
      ((sjmw/protobuf->hash handler-fn proto-class topic-entity-name) {:message {:left left-proto-message :right right-proto-message} :metadata {:topic "topic" :timestamp 1234567890 :partition 1}})
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
          handler-fn          (fn [{:keys [message metadata]}]
                                (when (and (= (:left message) left-message)
                                           (= (:right message) right-message)
                                           (= (:topic metadata) "topic")
                                           (= (:timestamp metadata) 1234567890)
                                           (= (:partition metadata) 1))
                                  (reset! handler-fn-called? true)))]
      ((sjmw/protobuf->hash handler-fn [proto-class proto-class] topic-entity-name) {:message {:left left-proto-message :right right-proto-message} :metadata {:topic "topic" :timestamp 1234567890 :partition 1}})
      (is (true? @handler-fn-called?)))))
