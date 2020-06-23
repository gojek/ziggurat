(ns ziggurat.messaging.rabbitmq.retry
  (:require [clojure.set :as set]))

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

(def valid-with-retry-args #{:count
                             :wait
                             :on-failure})

(defmacro with-retry [{:keys [count wait on-failure] :as opts} & body]
  (let [arg-diff (set/difference (set (keys opts))
                                 valid-with-retry-args)]
    (assert (= #{} arg-diff) (str "Valid args: " (vec valid-with-retry-args))))
  `(with-retry* {:count         (or ~count default-retry)
                 :wait          (or ~wait default-wait)
                 :fn-to-retry   (fn [] ~@body)
                 :fn-on-failure ~on-failure}))
