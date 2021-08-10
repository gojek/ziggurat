(ns ziggurat.header-transformer
  (:import [org.apache.kafka.streams.kstream ValueTransformer]
           [org.apache.kafka.streams.processor ProcessorContext]))

(deftype HeaderTransformer [^{:volatile-mutable true} processor-context] ValueTransformer
         (^void init [_ ^ProcessorContext context]
           (set! processor-context context))
         (transform [_ record-value]
           (let [topic     (.topic processor-context)
                 timestamp (.timestamp processor-context)
                 partition (.partition processor-context)
                 headers   (.headers processor-context)
                 metadata  {:topic topic :timestamp timestamp :partition partition}]
             {:value record-value :headers headers :metadata metadata}))
         (close [_] nil))

(defn create []
  (HeaderTransformer. nil))
