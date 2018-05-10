(ns ziggurat.external.yggdrasil
  (:require [cemerick.url :refer [url]]
            [clj-http.client :as http]
            [clojure.data.json :as json])
  (:import (java.net ConnectException)))

(defn- get-url [host port app-name]
  (str (url (str host ":" port) "/v1/configurations" app-name "latest")))

(defn get-config
  [app-name host port env connection-timeout-in-ms]
  (try (let [call-url (get-url host port app-name)
        response (http/get call-url {:socket-timeout   connection-timeout-in-ms
                                     :conn-timeout     connection-timeout-in-ms
                                     :query-params     {"q" env}
                                     :throw-exceptions false})]
    (if (http/success? response)
      (-> (reduce (fn [acc-map [k v]]
                    (assoc acc-map (-> k
                                       (clojure.string/replace #"_" "-")
                                       keyword) v)) {} (json/read-str (:body response)))
          :data)
      nil))
       (catch ConnectException e nil)))