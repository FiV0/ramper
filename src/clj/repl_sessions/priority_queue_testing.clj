(ns repl-sessions.priority-queue-testing
  (:require [criterium.core :as criterium]
            [ramper.util.priority-queue :as priority-queue]))

(def data (for [i (range 100000)] {:data :foo :prio i}))
(def prio-queue (priority-queue/priority-queue (fn [m] (:prio m)) data))

(criterium/with-progress-reporting (criterium/bench (seq data)))
;; mean 23.4 ns

(criterium/with-progress-reporting (criterium/bench (.isEmpty data)))
;; mean 1 ms

(def underlying-queue (.queue prio-queue))

(criterium/with-progress-reporting (criterium/bench (seq underlying-queue)))
;; mean 544.809 nano secs

(criterium/with-progress-reporting (criterium/bench (.isEmpty underlying-queue)))
;; mean 1.429 micro secs

(criterium/with-progress-reporting (criterium/bench (seq prio-queue)))
