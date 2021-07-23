(ns ziggurat.header-transformer-test
  (:require [clojure.test :refer [deftest is testing]]
            [ziggurat.header-transformer :refer [create]])
  (:import [org.apache.kafka.common.header.internals RecordHeaders RecordHeader]
           [org.apache.kafka.streams.processor ProcessorContext]))

(deftest header-transformer-test
  (testing "transforms value with passed headers"
    (let [headers         (RecordHeaders. (list (RecordHeader. "key" (byte-array (map byte "value")))))
          context         (reify ProcessorContext
                            (headers [_] headers)
                            (timestamp [_] 1234567890)
                            (topic [_] "topic")
                            (partition [_] 1))
          transformer     (create)
          _               (.init transformer context)
          metadata        {:topic "topic" :partition 1 :timestamp 1234567890}
          transformed-val (.transform transformer "val")]
      (is (= {:value "val" :headers headers :metadata metadata} transformed-val))))

  (testing "transforms value with nil headers when not passed"
    (let [context         (reify ProcessorContext
                            (headers [_] nil)
                            (timestamp [_] 1234567890)
                            (topic [_] "topic")
                            (partition [_] 1))
          transformer     (create)
          _               (.init transformer context)
          metadata        {:topic "topic" :partition 1 :timestamp 1234567890}
          transformed-val (.transform transformer "val")]
      (is (= {:value "val" :headers nil :metadata metadata} transformed-val)))))
