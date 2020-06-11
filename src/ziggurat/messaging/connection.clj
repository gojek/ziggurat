(ns ziggurat.messaging.connection
  (:require [clojure.tools.logging :as log]
            [langohr.core :as rmq]
            [mount.core :as mount :refer [defstate start]]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.channel :refer [get-keys-for-topic]])
  (:import [com.rabbitmq.client ShutdownListener]
           [java.util.concurrent Executors]))

