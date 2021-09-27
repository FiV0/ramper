(ns user
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clj-async-profiler.core :as prof]))

(s/check-asserts true)

(comment
  (set! *warn-on-reflection* true)

  (prof/start {})

  (def result (prof/stop {}))

  (prof/serve-files 8080)

  (refresh))
