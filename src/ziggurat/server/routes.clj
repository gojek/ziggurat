(ns ziggurat.server.routes
  (:require [bidi.ring :as bidi]
            [new-reliquary.ring :refer [wrap-newrelic-transaction]]
            [ring.middleware.defaults :as ring-defaults]
            [ring.middleware.json :refer [wrap-json-params wrap-json-response]]
            [ring.middleware.multipart-params :refer [wrap-multipart-params]]
            [ring.logger :refer [wrap-with-logger]]
            [ring.swagger.swagger-ui :as rsui]
            [ziggurat.resource.dead-set :as ds]
            [ziggurat.server.middleware :as m]
            [ziggurat.config :refer [get-in-config]]))

(defn ping [_request]
  {:status  200
   :headers {"Content-Type" "text/plain"}
   :body    "pong"})

(def routes-prefix ["/"])

(defn get-routes []
  [["ping" {:get ping}]
   ["v1/dead_set" {:delete (ds/delete-messages)}]
   ["v1/dead_set/replay" {:post (ds/get-replay)}]
   ["v1/dead_set" {:get (ds/get-view)}]
   [true (fn [_req] (ring.util.response/not-found ""))]])

(defn- swagger-enabled? [] (get-in-config [:swagger :enabled]))

(defn handler [actor-routes]
  (-> routes-prefix
      (conj (vec (concat actor-routes (get-routes))))
      (bidi/make-handler)
      (m/wrap-hyphenate)
      (cond->
       (swagger-enabled?) (rsui/wrap-swagger-ui {:path "/swagger-ui"}))
      (ring-defaults/wrap-defaults ring-defaults/api-defaults)
      (wrap-json-params)
      (wrap-multipart-params)
      (wrap-json-response)
      (m/wrap-default-content-type-json)
      (wrap-newrelic-transaction)
      (m/wrap-errors)
      (wrap-with-logger)))
