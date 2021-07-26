(ns ramper.frontier
  "The frontier contains a certain number datastructures shared across different
  threads of an agent."
  (:require [ramper.frontier.workbench :as workbench]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.delay-queue :as delay-queue]
            [ramper.util.lru-immutable :as lru]
            [ramper.util.url :as url]))

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

(def url-cache
  "An url cache for the urls seen recently. This avoids overloading the sieve and
  catches (apparently) already 90% of duplicates."
  (lru/create-lru-cache (runtime-config/approximate-url-cache-threshold) url/hash-url))

;; TODO maybe move to ramper.workers.dns-resolving
(def unknown-hosts
  "A queue of unknown hosts. A queue used by the dns resolving threads."
  (atom (delay-queue/delay-queue)))

(def new-visit-states
  "A queue of new visit states. Filled by the distributor and dequeued by
  dns resolving threads."
  (atom clojure.lang.PersistentQueue/EMPTY))

(defn intiailze-frontier
  "Initializes the frontiers datastructures."
  []
  (reset! refill-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! done-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! todo-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! results-queue clojure.lang.PersistentQueue/EMPTY)
  (reset! workbench (workbench/workbench))
  ;; TODO find a more elegent way
  (alter-var-root #'url-cache (fn [_] (lru/create-lru-cache
                                       (runtime-config/approximate-url-cache-threshold)
                                       url/hash-url)))
  (reset! unknown-hosts (delay-queue/delay-queue))
  (reset! new-visit-states clojure.lang.PersistentQueue/EMPTY))
