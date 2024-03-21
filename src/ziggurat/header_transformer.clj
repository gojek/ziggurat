(ns ziggurat.header-transformer
  (:import [org.apache.kafka.streams.kstream ValueTransformerWithKey]
           [org.apache.kafka.streams.processor ProcessorContext]))

(deftype HeaderTransformer [^{:volatile-mutable true} processor-context] ValueTransformerWithKey
         (^void init [_ ^ProcessorContext context]
           (set! processor-context context))
         (transform [_ record-key record-value]
           (let [topic     (.topic processor-context)
                 timestamp (.timestamp processor-context)
                 partition (.partition processor-context)
                 headers   (.headers processor-context)
                 metadata  {:topic topic :timestamp timestamp :partition partition}]
             {:value record-value :headers headers :metadata metadata :key record-key}))
         (close [_] nil))

(defn create []
  (HeaderTransformer. nil))
