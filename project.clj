(defproject com.gojek.lambda/ziggurat "2.7.2"
  :description "The actor framework for Project Lambda"
  :url "https://github.com/gojektech/ziggurat"
  :license {:name "Apache License, Version 2.0"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[bidi "2.1.4"]
                 [camel-snake-kebab "0.4.0"]
                 [cheshire "5.8.1"]
                 [clonfig "0.2.0"]
                 [clj-http "3.7.0"]
                 [com.cemerick/url "0.1.1"]
                 [com.datadoghq/java-dogstatsd-client "2.4"]
                 [com.fasterxml.jackson.core/jackson-databind "2.9.3"]
                 [com.novemberain/langohr "5.0.0"]
                 [com.taoensso/carmine "2.17.0"]
                 [io.dropwizard.metrics5/metrics-core "5.0.0-rc2" :scope "compile"]
                 [medley "0.8.4"]
                 [mount "0.1.10"]
                 [org.apache.httpcomponents/fluent-hc "4.5.4"]
                 [org.apache.kafka/kafka-streams "0.11.0.1" :exclusions [org.slf4j/slf4j-log4j12 log4j]]
                 [org.apache.logging.log4j/log4j-core "2.7"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.7"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [org.flatland/protobuf "0.8.1"]
                 [prismatic/schema "1.1.9"]
                 [ring/ring "1.6.3"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-defaults "0.3.1"]
                 [ring/ring-jetty-adapter "1.6.3"]
                 [ring/ring-json "0.4.0"]
                 [ring-logger "0.7.7"]
                 [tech.gojek/sentry-clj.async "1.0.0"]
                 [yleisradio/new-reliquary "1.0.0"]]
  :java-source-paths ["src/com"]
  :aliases {"test-all" ["with-profile" "default:+1.8:+1.9" "test"]}
  :profiles {:uberjar {:aot         :all
                       :global-vars {*warn-on-reflection* true}}
             :test    {:jvm-opts ["-Dlog4j.configurationFile=resources/log4j2.test.xml"]}
             :dev     {:plugins  [[jonase/eastwood "0.2.6"]
                                  [lein-githooks "0.1.0"]
                                  [lein-kibit "0.1.6"]
                                  [lein-cljfmt "0.6.3"]]
                       :githooks {:auto-install true
                                  :pre-commit   ["lein cljfmt check && lein kibit"]
                                  :pre-push     ["lein test"]}}
             :1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}
             :1.8     {:dependencies [[org.clojure/clojure "1.8.0"]]}})
