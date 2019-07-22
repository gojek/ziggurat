(defproject tech.gojek/ziggurat "4.0.0-alpha.4"
  :description "A stream processing framework to build stateless applications on kafka"
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
                 [com.taoensso/nippy "2.14.0"]
                 [io.dropwizard.metrics5/metrics-core "5.0.0-rc2" :scope "compile"]
                 [medley "0.8.4"]
                 [mount "0.1.10"]
                 [org.apache.httpcomponents/fluent-hc "4.5.4"]
                 [org.apache.kafka/kafka-streams "2.1.0" :exclusions [org.slf4j/slf4j-log4j12 log4j]]
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
  :deploy-repositories [["clojars" {:url "https://clojars.org/repo"
                                      :username :env/clojars_username
                                      :password :env/clojars_password
                                      :sign-releases false}]]
  :java-source-paths ["src/com"]
  :aliases {"test-all"                   ["with-profile" "default:+1.8:+1.9" "test"]
            "code-coverage"              ["with-profile" "test" "cloverage" "--output" "coverage" "--coveralls"]
            "run-junit"                  ["do" "clean," "install," "with-profile" "test" "junit"]}
            ;"test-with-junit"            ["do" "clean," "test," "install," "with-profile" "test" "junit"]
            ;"test-all-with-junit"        ["do" "clean," "test-all," "install," "with-profile" "test" "junit"]}
  :aot :all
  :profiles {:uberjar {:aot         :all
                       :global-vars {*warn-on-reflection* true}}
             :test    {:jvm-opts     ["-Dlog4j.configurationFile=resources/log4j2.test.xml"]
                       :dependencies [[com.google.protobuf/protobuf-java "3.5.1"]
                                      [io.confluent/kafka-schema-registry "4.1.1"]
                                      [junit/junit "4.12"]
                                      [org.apache.kafka/kafka-streams "2.1.0" :classifier "test" :exclusions [org.slf4j/slf4j-log4j12 log4j]]
                                      [org.apache.kafka/kafka-clients "2.1.0" :classifier "test"]
                                      [org.apache.kafka/kafka_2.11 "2.1.0" :classifier "test"]
                                      [org.clojure/test.check "0.9.0"]
                                      [tech.gojek/ziggurat-test-utils "1.0.0-SNAPSHOT" :scope "test"]]
                       :plugins      [[lein-cloverage "1.0.13"]
                                      [lein-junit "1.1.9"]]
                       :junit ["test"]
                       :repositories [["confluent-repo" "https://packages.confluent.io/maven/"]]
                       :aot :all
                       :java-source-paths ["src/com", "test/tech"]}
             :dev     {:plugins  [[lein-cljfmt "0.6.3"]
                                  [lein-cloverage "1.0.13"]
                                  [lein-kibit "0.1.6"]]}
             :1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}
             :1.8     {:dependencies [[org.clojure/clojure "1.8.0"]]}})
