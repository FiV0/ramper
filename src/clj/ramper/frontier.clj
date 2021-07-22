(ns ramper.frontier
  "The frontier contains a certain number datastructures shared across different
  threads of an agent."
  (:require [ramper.frontier.workbench :as workbench]))

(def refill-queue
  "A queue containing visit states that can be refilled. Filled in the \"done\"
  thread and dequeued from the distributor."
  (atom clojure.lang.PersistentQueue/EMPTY))

(def done-queue
  "A queue containing visit states coming from fetching threads and dequeued
  by the \"done\" thread."
  (atom clojure.lang.PersistentQueue/EMPTY))

(def todo-queue
  "A queue containing visit states that are ready for fetching. Filled from the
  \"todo\" thread and dequeued from fetching threads."
  (atom clojure.lang.PersistentQueue/EMPTY))

;; TODO: define fetched data! extra type? just map of stuff
;; going for {:url ... :response} for now
(def results-queue
  "A queue containing the fetched data that is ready for further processing.
  Filled by fetching threads, emptied by parsing threads."
  (atom clojure.lang.PersistentQueue/EMPTY))

(def workbench
  "The workbench manages visit states that are ready for fetching. The
  workbench is filled from the distributor, \"dns\" threads or the
  \"done\" thread. The \"todo\" thread dequeues from it. See also
  ramper.frontier.workbench."
  (atom (workbench/workbench)))

(defn intiailze-frontier
  "Initializes the frontiers datastructures."
  []
  (reset! refill-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! done-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! todo-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! results-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! workbench (workbench/workbench)))
