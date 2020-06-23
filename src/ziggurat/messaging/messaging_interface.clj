(ns ziggurat.messaging.messaging-interface)

(defprotocol MessagingProtocol
  (start-connection [config stream-routes])
  (stop-connection [connection config stream-routes])
  (publish [exchange message-payload])
  (publish [exchange message-payload expiration])
  (create-and-bind-queue [queue-name exchange-name])
  (create-and-bind-queue [queue-name exchange-name dead-letter-exchange])
  (get-messages-from-queue [queue-name ack?])
  (get-messages-from-queue [queue-name ack? count])
  (process-messages-from-queue [queue-name count processing-fn])
  (start-subscriber [prefetch-count wrapped-mapper-fn queue-name])
  (consume-message [ch meta payload ack?]))
