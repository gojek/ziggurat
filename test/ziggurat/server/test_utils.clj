(ns ziggurat.server.test-utils
  (:refer-clojure :exclude [get])
  (:require [camel-snake-kebab.core :as csk]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.tools.logging :as log]
            [ziggurat.server.map :as umap]))

(defn- mk-url [port path]
  (str "http://localhost:" port path))

(defn- wrap-bad-status-logging [client]
  (fn [{:keys [success-expected?] :as req}]
    (let [resp (client req)]
      (if (and success-expected? (not (http/success? resp)))
        (let [resp_ (http/coerce-response-body req resp)]
          (log/warn "HTTP call returned bad status" {:request req :response resp_})
          (update resp_ :body #(.getBytes %)))
        resp))))

(defn get
  ([port path]
   (get port path true))
  ([port path success-expected?]
   (get port path success-expected? true))
  ([port path success-expected? json-content?]
   (get port path success-expected? json-content? {}))
  ([port path success-expected? json-content? headers]
   (get port path success-expected? json-content? headers {}))
  ([port path success-expected? json-content? headers query-params]
   (http/with-additional-middleware [#'wrap-bad-status-logging]
     (let [resp (http/get (mk-url port path) {:success-expected? success-expected?
                                              :throw-exceptions  false
                                              :headers           headers
                                              :query-params      query-params})]
       (cond-> resp
         json-content?
         (update :body #(umap/nested-map-keys (fn [k] (csk/->kebab-case-keyword k :separator \_))
                                              (json/decode %))))))))

(defn post
  ([port path]
   (post port path nil))
  ([port path obj]
   (post port path true obj))
  ([port path success-expected? obj]
   (post port path success-expected? obj {}))
  ([port path success-expected? obj headers]
   (http/with-additional-middleware [#'wrap-bad-status-logging]
     (http/post (mk-url port path)
                {:body              (json/encode obj)
                 :content-type      :json
                 :success-expected? success-expected?
                 :throw-exceptions  false
                 :headers           headers}))))

(defn put
  ([port path obj]
   (put port path true obj))
  ([port path success-expected? obj]
   (put port path success-expected? obj {}))
  ([port path success-expected? obj headers]
   (http/with-additional-middleware [#'wrap-bad-status-logging]
     (http/put (mk-url port path)
               {:body              (json/encode obj)
                :content-type      :json
                :success-expected? success-expected?
                :throw-exceptions  false
                :headers           headers}))))
