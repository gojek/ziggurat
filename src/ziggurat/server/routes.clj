(ns ziggurat.server.routes
  (:require [bidi.ring :as bidi]
            [ziggurat.server.middleware :as m]
            [ziggurat.resource.dead-set :as ds]
            [new-reliquary.ring :refer [wrap-newrelic-transaction]]
            [ring.middleware.defaults :as ring-defaults]
            [ring.middleware.json :refer [wrap-json-params wrap-json-response]]
            [clojure.tools.logging :as log]
            [ring.logger :refer [wrap-with-logger]]
            [ring.util.response :as ring-resp]
            [schema.core :as s]))

(defn ping [_request]
  {:status  200
   :headers {"Content-Type" "text/plain"}
   :body    "pong"})

(def routes
  ["/" [["ping" {:get ping}]
        ["v1/dead_set/replay" {:post ds/replay}]
        ["v1/dead_set" {:get ds/view}]
        [true (fn [_req] (ring-resp/not-found ""))]]])

(defn handler []
  (-> routes
      (bidi/make-handler)
      (m/wrap-hyphenate)
      (ring-defaults/wrap-defaults ring-defaults/api-defaults)
      (wrap-json-params)
      (wrap-json-response)
      (m/wrap-default-content-type-json)
      (wrap-newrelic-transaction)
      (m/wrap-errors)
      (wrap-with-logger)))
