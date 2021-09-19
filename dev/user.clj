(ns user
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.namespace.repl :refer [refresh]]))

(s/check-asserts true)

(comment
  (refresh))
