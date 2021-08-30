(ns ramper.frontier
  "The frontier contains a certain number datastructures shared across different
  threads of an agent."
  (:require [clojure.java.io :as io]
            [lambdaisland.uri :as uri]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtualizer]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.sieve :as sieve]
            [ramper.sieve.disk-flow-receiver :as receiver]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.startup-configuration :as startup-config]
            [ramper.store.parallel-buffered-store :as store]
            [ramper.util.data-disk-queues :as ddq]
            [ramper.util.delay-queue :as delay-queue]
            [ramper.util.lru :as lru]
            [ramper.util.lru-immutable :as lru-immutable]
            [ramper.util.url :as url]))

;; TODO maybe create a frontier that holds all the below datastructures
;; and pass that around

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

(defn- frontier-subdir [runtime-config name]
  (io/file (:ramper/frontier-dir runtime-config) name))

(defn workbench-virtualizer-init
  ([] (workbench-virtualizer-init @runtime-config/runtime-config))
  ([runtime-config]
   (let [virtualizer-subdir (frontier-subdir runtime-config "virtualizer")]
     (when-not (.exists virtualizer-subdir)
       (.mkdirs virtualizer-subdir))
     (virtualizer/workbench-virtualizer virtualizer-subdir))))

(def workbench-virtualizer
  "The virtualizer for the workbench."
  (atom nil #_(workbench-virtualizer-init)))

;; TODO add init urls to cache
(def url-cache
  "An url cache for the urls seen recently. This avoids overloading the sieve and
  catches (apparently) already 90% of duplicates."
  (lru-immutable/create-lru-cache (runtime-config/approximate-url-cache-threshold) url/hash-url-128))

;; TODO maybe move to ramper.workers.dns-resolving
(def unknown-hosts
  "A queue of unknown hosts. A queue used by the dns resolving threads."
  (atom (delay-queue/delay-queue)))

(def new-visit-states
  "A queue of new visit states. Filled by the distributor and dequeued by
  dns resolving threads."
  (atom clojure.lang.PersistentQueue/EMPTY))

(defn- url-flow-receiver-init []
  (receiver/disk-flow-receiver (url/url-byte-serializer)))

;; TODO: Does this need to be in atom. Can the receiver not implement a reset.
;; TODO: Move dequeue protocol to better place.
;; TODO: Could this not always be got from the sieve itself.
(def ready-urls
  " A (probably disk-based) queue to store urls coming out of the sieve.

  Should implement `ramper.sieve.flow-receiver.FlowReceiver` as well as (for now)
  `ramper.sieve.disk-flow-receiver.DiskFlowReceiverDequeue`"
  (atom nil #_(url-flow-receiver-init)))

(defn- sieve-init
  ([] (sieve-init @runtime-config/runtime-config @ready-urls))
  ([runtime-config receiver]
   (mercator-sieve/mercator-seive
    (:ramper/is-new runtime-config)
    (runtime-config/sieve-dir runtime-config)
    (:ramper/sieve-size runtime-config)
    (:ramper/store-buffer-size runtime-config)
    (:ramper/aux-buffer-size runtime-config)
    receiver
    (url/url-byte-serializer)
    url/hash-url)))

(def sieve
  "An url sieve implementing `ramper.sieve.Sieve`."
  (atom nil #_(sieve-init)))

(defn- store-init
  ([] (store-init @runtime-config/runtime-config))
  ([runtime-config]
   (store/parallel-buffered-store
    (:ramper/store-dir runtime-config)
    (:ramper/is-new runtime-config)
    (:ramper/store-buffer-size runtime-config))))

(def store
  "A response store that must implement ramper.store/Store and probably should implement
  ramper.store/StoreReader."
  (atom nil #_(store-init)))

(defn- data-disk-queues-init [name]
  (ddq/data-disk-queues (io/file (:ramper/frontier-dir @runtime-config/runtime-config) name)))

;; TODO not used for now
(def received-urls
  "A (probably disk-based) queue to store urls coming from different agents."
  (atom nil #_(data-disk-queues-init "received")))

(def path-queries-in-queues
  "The overall number of path queries stored in visit-state queues."
  (atom 0))

;; TODO not used for now
;; TODO: Do we really need this? Maybe otherwise to much load on memory.
;; Should we just an approximate amount of path+queries that should reside in
;; memory? See runtime-config/workbench-size-in-path-queries.
(def weight-of-path-queries
  "The weight of all the path+queris stored in visit-state queues."
  (atom 0))

(def urls-crawled
  "The total number of urls crawled and stored."
  (atom 0))

(def scheme+authority-to-count
  "A map from scheme+authority to number of urls crawled (or to be crawled) so far.

  Mainly used in the context `:ramper/max-urls-per-scheme+authority`"
  (atom {}))

(defn initiailze-frontier
  "Initializes the frontiers datastructures."
  ([] (initiailze-frontier @runtime-config/runtime-config))
  ([runtime-config]
   (reset! refill-queue clojure.lang.PersistentQueue/EMPTY)
   (reset! done-queue clojure.lang.PersistentQueue/EMPTY)
   (reset! todo-queue clojure.lang.PersistentQueue/EMPTY)
   (reset! results-queue clojure.lang.PersistentQueue/EMPTY)
   (reset! workbench (workbench/workbench))
   (reset! workbench-virtualizer (workbench-virtualizer-init))
   ;; TODO find a more elegent way
   (alter-var-root #'url-cache (fn [_] (lru-immutable/create-lru-cache
                                        (runtime-config/approximate-url-cache-threshold runtime-config)
                                        url/hash-url-128)))
   (reset! unknown-hosts (delay-queue/delay-queue))
   (reset! new-visit-states clojure.lang.PersistentQueue/EMPTY)
   (reset! ready-urls (url-flow-receiver-init))
   (reset! sieve (sieve-init))
   (reset! store (store-init))
   (reset! received-urls (data-disk-queues-init "received"))
   (reset! path-queries-in-queues 0)
   (reset! weight-of-path-queries 0)
   (reset! urls-crawled 0)
   (reset! scheme+authority-to-count {})))

(defn workbench-full?
  "Returns true if the workbench is considered full."
  ([] (workbench-full? @runtime-config/runtime-config @path-queries-in-queues))
  ([runtime-config path-queries-in-queues]
   #_(<= (:ramper/workbench-max-byte-size @runtime-config/runtime-config) @weight-of-path-queries)
   (<= (runtime-config/workbench-size-in-path-queries runtime-config) path-queries-in-queues)))

(defrecord Frontier [refill-queue done-queue todo-queue results-queue
                     workbench virtualizer url-cache unknown-hosts
                     new-visit-states ready-urls sieve store
                     path-queries-in-queues urls-crawled scheme+authority-to-count])

(defn frontier
  "Creates a frontier initialized with all the data-structures used by an agent."
  [runtime-config]
  (let [seed-urls (startup-config/read-urls* (:ramper/seed-file runtime-config))
        url-cache (lru-immutable/create-lru-cache
                   (runtime-config/approximate-url-cache-threshold runtime-config)
                   url/hash-url-128)
        ready-urls (url-flow-receiver-init)
        sieve (sieve-init runtime-config ready-urls)]
    (run! #(lru/add url-cache %) seed-urls)
    (run! #(sieve/enqueue sieve %) seed-urls)
    (->Frontier (atom clojure.lang.PersistentQueue/EMPTY)
                (atom clojure.lang.PersistentQueue/EMPTY)
                (atom clojure.lang.PersistentQueue/EMPTY)
                (atom clojure.lang.PersistentQueue/EMPTY)
                (atom (workbench/workbench))
                (workbench-virtualizer-init runtime-config)
                url-cache
                (atom (delay-queue/delay-queue))
                (atom clojure.lang.PersistentQueue/EMPTY)
                ready-urls
                sieve
                (store-init runtime-config)
                (atom 0)
                (atom 0)
                (atom {}))))

(defn stop?
  "Returns true when the crawler should be stopped due to some stopping
  conditions being met."
  [runtime-config {:keys [urls-crawled] :as _frontier}]
  (if (contains? @runtime-config :ramper/max-urls)
    (> (:ramper/max-urls @runtime-config) @urls-crawled)
    false))
