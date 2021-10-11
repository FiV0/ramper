(ns ramper.frontier.workbench.virtualizer
  "This namespace helps to \"virtualize\" the workbench so that it's memory footprint
  remains small. It is is essentially a mapping from scheme+authority to path queries.
  Whenever path queries leave the sieve that currently don't fit into the workbench, they
  get written to disk by the virtualizer. When a visit state needs get refilled the
  path queries for that visit state get read as bulk from disk and appended to the visit
  state.

  Underneath the virtualizer maps a scheme+authority to a queue of path queries. These
  path queries a written to append only log files. Garbage collection can be performed
  whenever the ratio occupied and actually used space drops below some threshold.

  IMPORTANT! This structure is not thread-safe."
  (:refer-clojure :exclude [count remove])
  (:require [io.pedestal.log :as log]
            [ramper.frontier.workbench2 :as workbench]
            [ramper.util :as util]
            [ramper.util.url :as url])
  (:import (java.io Closeable)
           (ramper.util ByteArrayDiskQueues)))

;; WorkbenchVirtualizer documentation
;;
;; disk-queues - the visit-state to path-queries mapping
;; directory - the directory where the log files reside

;; TODO add Metadata saving on close for restart
;; TODO unify this with ramper.util.DataDiskQueues

(defrecord WorkbenchVirtualizer [disk-queues directory]
  Closeable
  (close [_this]
    (.close ^ByteArrayDiskQueues disk-queues)))

(defn workbench-virtualizer
  "Creates a WorkbenchVirtualizer instance."
  [dir]
  (->WorkbenchVirtualizer (ByteArrayDiskQueues. dir) dir))

(defn- entry-key [{:keys [scheme+authority] :as _entry}]
  (-> scheme+authority str hash))

(defn dequeue-path-queries
  "Dequeues a maximum number of `max-urls` of path+queries from the
  `virtualizer` to the `visit-state`."
  [^WorkbenchVirtualizer {:keys [^ByteArrayDiskQueues disk-queues] :as _virtualizer}
   entry max-urls]
  (io!
   (let [key (entry-key entry)]
     (loop [entry entry to-go (min max-urls (.count disk-queues key))]
       (if (pos? to-go)
         (recur (update entry :path-queries conj (util/bytes->string (.dequeue disk-queues key)))
                (dec to-go))
         entry)))))

(defn count
  "Returns the number of path+queries associated with a visit-state."
  [^WorkbenchVirtualizer {:keys [disk-queues] :as _virtualizer} entry]
  (.count ^ByteArrayDiskQueues disk-queues (entry-key entry)))

(defn on-disk
  "Returns the number of visit-states on disk."
  [^WorkbenchVirtualizer {:keys [disk-queues] :as _virtualizer}]
  (.numKeys ^ByteArrayDiskQueues disk-queues))

(defn remove
  "Removes all path+queries associated with the given visit state."
  [^WorkbenchVirtualizer {:keys [disk-queues] :as _virtualizer} entry]
  (io!
   (.remove ^ByteArrayDiskQueues disk-queues entry)))

;; TODO is the visit-state here really necessary?
(defn enqueue
  "Enqueues a given `url` to the virtualizer for the given `visit-state`."
  [^WorkbenchVirtualizer {:keys [disk-queues] :as _virtualizer} entry url]
  (io!
   (.enqueue ^ByteArrayDiskQueues disk-queues
             (entry-key entry)
             (-> url url/path+queries str util/string->bytes))))

(defn collect-if
  "Performs garbage collection if the space used is below `threshold` and tries to achieve space
  usage of `target-ratio`"
  ([^WorkbenchVirtualizer virtualizer] (collect-if virtualizer 0.5 0.75))
  ([^WorkbenchVirtualizer {:keys [disk-queues] :as _virtualizer} threshold target-ratio]
   (when (< (.ratio ^ByteArrayDiskQueues disk-queues) threshold)
     (io!
      (log/info :workbench-virtualizer "Start collection ...")
      (.collect ^ByteArrayDiskQueues disk-queues target-ratio)
      (log/info :workbench-virtualizer "Completed collection ...")))))
