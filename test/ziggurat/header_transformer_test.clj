(ns ziggurat.header-transformer-test
  (:require [clojure.test :refer :all]
            [ziggurat.header-transformer :refer :all])
  (:import [org.apache.kafka.streams.processor ProcessorContext]
           [org.apache.kafka.common.header.internals RecordHeaders RecordHeader]))

(deftest header-transformer-test
  (testing "transforms value with passed headers"
    (let [headers (RecordHeaders. (list (RecordHeader. "key" (byte-array (map byte "value")))))
          context (reify ProcessorContext
                    (headers [_] headers))
          transformer (create)
          _ (.init transformer context)
          transformed-val (.transform transformer "val")]
      (is (= {:value "val" :headers headers} transformed-val))))

  (testing "transforms value with nil headers when not passed"
    (let [context (reify ProcessorContext
                    (headers [_] nil))
          transformer (create)
          _ (.init transformer context)
          transformed-val (.transform transformer "val")]
      (is (= {:value "val" :headers nil} transformed-val)))))
