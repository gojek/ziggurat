(ns ziggurat.channel)

(defn get-keys-for-topic [stream-routes topic-entity]
  (-> stream-routes
                        (get topic-entity)
             (dissoc :handler-fn)
      keys))