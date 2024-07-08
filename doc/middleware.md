## Middlewares in Ziggurat

Version 3.0.0 of Ziggurat introduces the support of Middleware. Old versions of Ziggurat (< 3.0) assumed that the messages read from kafka were serialized in proto-format and thus it deserialized
them and passed a clojure map to the mapper-fn. We have now pulled the deserialization function into a middleware and users have the freedom to use this function to deserialize their messages
or define their custom middlewares. This enables ziggurat to process messages serialized in any format.

### Custom Middleware usage

The default middleware `default/protobuf->hash` assumes that the message is serialized in proto format.

```clojure
(require '[ziggurat.init :as ziggurat])

(defn start-fn []
    ;; your logic that runs at startup goes here
)

(defn stop-fn []
    ;; your logic that runs at shutdown goes here
)

(defn main-fn
  [{:keys [message metadata] :as message-payload}]
    (println message)
    :success)

(defn wrap-middleware-fn
    [mapper-fn :stream-id]
    (fn [message]
      (println "processing message for stream: " :stream-id)
      (mapper-fn (deserialize-message message))))

(def handler-fn
    (-> main-fn
      (wrap-middleware-fn :stream-id)))

(ziggurat/main start-fn stop-fn {:stream-id {:handler-fn handler-fn}})
```

_The handler-fn gets a serialized message from kafka and thus we need a deserialize-message function. We have provided default deserializers in Ziggurat_

### Deserializing JSON messages using JSON middleware

Ziggurat 3.1.0 provides a middleware to deserialize JSON messages, along with proto.
It can be used like this.

```clojure
(def message-handler-fn
  (-> actual-message-handler-function
      (parse-json :stream-route-config)))
```

Here, `message-handler-fn` calls `parse-json` with a message handler function
`actual-message-handler-function` as the first argument and the key of a stream-route
config (as defined in `config.edn`) as the second argument.
