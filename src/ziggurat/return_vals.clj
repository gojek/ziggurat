(ns ziggurat.return-vals
  (:gen-class
    :name ziggurat.ReturnVals
    :methods [^{:static true} [success [] clojure.lang.Keyword]
              ^{:static true} [retry [] clojure.lang.Keyword]
              ^{:static true} [skip [] clojure.lang.Keyword]]))



(defn -success []
  :success)
(defn -retry []
  :retry)
(defn -skip []
  :skip)