(ns ziggurat.header-transformer
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.kafka.streams.kstream ValueTransformer]
           [org.apache.kafka.streams.processor ProcessorContext]))

(deftype HeaderTransformer [^{:volatile-mutable true} processor-context] ValueTransformer
         (^void init [_ ^ProcessorContext context]
           (do (set! processor-context context)
               nil))
         (transform [_ record-value]
           (log/info "record-value--> " record-value)
           {:value record-value :headers (.headers processor-context)})
         (close [_] nil))

(defn create []
  (HeaderTransformer. nil))
