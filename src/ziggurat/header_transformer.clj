(ns ziggurat.header-transformer
  (:import [org.apache.kafka.streams.kstream ValueTransformer]
           [org.apache.kafka.streams.processor ProcessorContext]))

(deftype HeaderTransformer [^{:volatile-mutable true} processor-context] ValueTransformer
         (^void init [_ ^ProcessorContext context]
           (set! processor-context context))
         (transform [_ record-value]
           (let [timestamp (.timestamp processor-context)
                 headers   (.headers processor-context)
                 topic     (.topic processor-context)
                 partition (.partition processor-context)
                 metadata  {:topic topic :partition partition :timestamp timestamp}]
             {:value record-value :headers headers :metadata metadata}))
         (close [_] nil))

(defn create []
  (HeaderTransformer. nil))
