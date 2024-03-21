(ns ziggurat.header-transformer-test
  (:require [clojure.test :refer [deftest is testing]]
            [ziggurat.header-transformer :refer [create]])
  (:import [org.apache.kafka.common.header.internals RecordHeaders RecordHeader]
           [org.apache.kafka.streams.processor ProcessorContext]))

(deftest header-transformer-test
  (testing "transforms value with passed headers"
    (let [headers         (RecordHeaders. (list (RecordHeader. "key" (byte-array (map byte "value")))))
          topic           "topic"
          timestamp       1234567890
          partition       1
          context         (reify ProcessorContext
                            (headers [_] headers)
                            (topic [_] topic)
                            (timestamp [_] timestamp)
                            (partition [_] partition))
          transformer     (create)
          _               (.init transformer context)
          transformed-val (.transform transformer "key" "val")]
      (is (= {:key "key" :value "val" :headers headers :metadata {:topic topic :timestamp timestamp :partition partition}} transformed-val))))

  (testing "transforms value with nil headers when not passed"
    (let [topic           "topic"
          timestamp       1234567890
          partition       1
          context         (reify ProcessorContext
                            (headers [_] nil)
                            (topic [_] topic)
                            (timestamp [_] timestamp)
                            (partition [_] partition))
          transformer     (create)
          _               (.init transformer context)
          transformed-val (.transform transformer "key" "val")]
      (is (= {:key "key" :value "val" :headers nil :metadata {:topic topic :timestamp timestamp :partition partition}} transformed-val)))))
