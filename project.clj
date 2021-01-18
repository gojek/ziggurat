;; allow insecure downloads
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory!
  "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))
(defproject tech.gojek/ziggurat "3.7.0"
  :description "A stream processing framework to build stateless applications on kafka"
  :url "https://github.com/gojektech/ziggurat"
  :license {:name "Apache License, Version 2.0"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[bidi "2.1.6"]
                 [camel-snake-kebab "0.4.0"]
                 [cheshire "5.9.0"]
                 [clonfig "0.2.0"]
                 [clj-http "3.10.0"]
                 [com.cemerick/url "0.1.1"]
                 [com.datadoghq/java-dogstatsd-client "2.4"]
                 [com.fasterxml.jackson.core/jackson-databind "2.9.9"]
                 [com.novemberain/langohr "5.1.0" :exclusions [org.clojure/clojure]]
                 [com.taoensso/nippy "2.14.0"]
                 [io.dropwizard.metrics5/metrics-core "5.0.0-rc2" :scope "compile"]
                 [medley "1.2.0" :exclusions [org.clojure/clojure]]
                 [mount "0.1.16"]
                 [io.jaegertracing/jaeger-core "1.0.0"]
                 [io.jaegertracing/jaeger-client "1.0.0"]
                 [io.opentracing/opentracing-api "0.33.0"]
                 [io.opentracing/opentracing-mock "0.33.0"]
                 [io.opentracing/opentracing-noop "0.33.0"]
                 [io.opentracing.contrib/opentracing-kafka-streams "0.1.4" :exclusions [org.lz4/lz4-java com.github.luben/zstd-jni org.apache.kafka/kafka-streams org.slf4j/slf4j-api org.xerial.snappy/snappy-java]]
                 [io.opentracing.contrib/opentracing-kafka-client "0.1.4" :exclusions [org.lz4/lz4-java com.github.luben/zstd-jni org.slf4j/slf4j-api org.xerial.snappy/snappy-java]]
                 [io.opentracing.contrib/opentracing-rabbitmq-client "0.1.5" :exclusions [com.rabbitmq/amqp-client]]
                 [org.apache.httpcomponents/fluent-hc "4.5.4"]
                 [org.apache.kafka/kafka-streams "2.3.0" :exclusions [org.slf4j/slf4j-log4j12 log4j]]
                 [org.apache.logging.log4j/log4j-core "2.12.1"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.12.1"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.5.0"]
                 [nrepl/nrepl "0.6.0"]
                 [clojusc/protobuf "3.5.1-v1.1"]
                 [org.flatland/protobuf "0.8.1"]
                 [prismatic/schema "1.1.12"]
                 [clj-statsd "0.4.0"]
                 [ring/ring "1.7.1"]
                 [ring/ring-core "1.7.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-jetty-adapter "1.7.1"]
                 [ring/ring-json "0.4.0"]
                 [ring-logger "0.7.7"]
                 [tech.gojek/sentry-clj.async "1.0.0" :exclusions [org.clojure/clojure]]
                 [com.newrelic.agent.java/newrelic-api "4.0.0"]
                 [yleisradio/new-reliquary "1.1.0" :exclusions [org.clojure/clojure]]
                 [metosin/ring-swagger "0.26.2"
                  :exclusions [cheshire
                               com.fasterxml.jackson.core/jackson-core
                               com.fasterxml.jackson.dataformat/jackson-dataformat-smile
                               com.fasterxml.jackson.dataformat/jackson-dataformat-cbor]]
                 [metosin/ring-swagger-ui "3.25.3"]]
  :deploy-repositories [["clojars" {:url           "https://clojars.org/repo"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]]
  :pedantic? :warn
  :java-source-paths ["src/com"]
  :aliases {"code-coverage" ["with-profile" "test" "cloverage" "--output" "coverage" "--lcov"]}
  :aot [ziggurat.init ziggurat.config ziggurat.producer ziggurat.sentry ziggurat.metrics ziggurat.fixtures]
  :profiles {:uberjar {:aot         :all
                       :global-vars {*warn-on-reflection* true}
                       :pedantic?   :abort}
             :test    {:java-source-paths ["src/com" "test/com"]
                       :jvm-opts     ["-Dlog4j.configurationFile=resources/log4j2.test.xml"]
                       :dependencies [[com.google.protobuf/protobuf-java "3.5.1"]
                                      [io.confluent/kafka-schema-registry "4.1.1" :exclusions [javax.ws.rs/javax.ws.rs-api com.fasterxml.jackson.core/jackson-annotations]]
                                      [junit/junit "4.12"]
                                      [org.apache.kafka/kafka-streams "2.3.0" :classifier "test" :exclusions [org.slf4j/slf4j-log4j12 log4j]]
                                      [org.apache.kafka/kafka-clients "2.3.0" :classifier "test"]
                                      [org.apache.kafka/kafka_2.11 "2.3.0" :classifier "test"]
                                      [org.clojure/test.check "0.10.0"]]
                       :plugins      [[lein-cloverage "1.0.13" :exclusions [org.clojure/clojure]]]
                       :repositories [["confluent-repo" "https://packages.confluent.io/maven/"]]}
             :dev     {:plugins [[lein-ancient "0.6.15"]
                                 [lein-cljfmt "0.6.4"]
                                 [lein-cloverage "1.0.13"]
                                 [lein-kibit "0.1.7" :exclusions [org.clojure/tools.cli rewrite-clj org.clojure/tools.reader]]]}})
