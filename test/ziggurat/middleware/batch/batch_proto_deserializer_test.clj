(ns ziggurat.middleware.batch.batch-proto-deserializer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.middleware.batch.batch-proto-deserializer :refer :all]
            [protobuf.core :as proto]
            [ziggurat.middleware.batch.batch-proto-deserializer :as mw])
  (:import (flatland.protobuf.test Example$Photo)))

(deftest batch-proto-deserializer-test
  (let [message            {:id 7 :path "/photos/h2k3j4h9h23"}
        proto-class        Example$Photo
        topic-entity-name  "test"
        batch-message      (repeat 10 (proto/->bytes (proto/create Example$Photo message)))
        handler-fn-called? (atom false)
        handler-fn         (fn [batch]
                             (is (seq? batch))
                             (loop [msg batch]
                               (when (not-empty msg)
                                (is (= message (first msg)))
                                (recur (rest msg))))
                             (reset! handler-fn-called? true))]
    ((mw/deserialize-batch-of-proto-messages handler-fn proto-class topic-entity-name) batch-message)
    (is (true? @handler-fn-called?))))
