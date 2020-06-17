(ns ziggurat.messaging.rabbitmq.producer-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :as util]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.config :as config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.config :as config]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.rabbitmq.producer :as rm-prod]
            [langohr.channel :as lch]
            [ziggurat.fixtures :as fix]
            [langohr.queue :as lq]
            [langohr.exchange :as le]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))


