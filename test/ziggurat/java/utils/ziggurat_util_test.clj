(ns ziggurat.java.utils.ziggurat-util-test
  (:require [clojure.test :refer :all]
            [ziggurat.java.utils.ziggurat-util :refer [-createClojureHashMap]]))

(defn- create-java-list []
  (doto (new java.util.ArrayList)
    (.add "123")))

(defn- create-java-array []
  (.toCharArray "Hello World"))

(defn simple-java-hash-map []
  (doto (new java.util.HashMap)
    (.put "first-key" "first value")
    (.put "second-key" (create-java-list))
    (.put "third-key"  (create-java-array))
    (.put "fourth-key" "just-another-value")))

(defn java-hash-map-with-a-hash-map []
  (doto (new java.util.HashMap)
    (.put "keyword" "value")
    (.put "another-keyword" (simple-java-hash-map))))

(deftest creates-stream-routes-map
  (testing "Should construct a clojure hash-map for stream routes from a Java HashMap"
    (let [clojure-hash-map (-createClojureHashMap (java-hash-map-with-a-hash-map))
          val (:keyword clojure-hash-map)
          val-hash-map (:another-keyword clojure-hash-map)]
      (is (= "value" val))
      (is (map? val-hash-map))
      (let [inner-val (:first-key val-hash-map)
            java-iterable-val (:second-key val-hash-map)
            java-array-val (:third-key val-hash-map)
            inner-val-fourth (:fourth-key val-hash-map)]
        (is (= "first value" inner-val))
        (is (seq? java-iterable-val))
        (is (seq? java-array-val))
        (is (= "just-another-value" inner-val-fourth))))))



