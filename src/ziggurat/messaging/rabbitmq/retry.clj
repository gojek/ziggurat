(ns ziggurat.messaging.rabbitmq.retry)

(def default-wait 100)
(def default-retry 2)

(defn with-retry* [{:keys [count wait fn-to-retry fn-on-failure]
                    :or   {count default-retry wait default-wait}}]
  (let [res (try
              (fn-to-retry)
              (catch Throwable e
                (when fn-on-failure
                  (fn-on-failure e))
                (if-not (zero? count)
                  ::try-again
                  (throw e))))]
    (if (= res ::try-again)
      (do
        (Thread/sleep wait)
        (recur {:count         (dec count)
                :wait          wait
                :fn-to-retry   fn-to-retry
                :fn-on-failure fn-on-failure}))
      res)))