(ns ramper.util.data-disk-queues
  "A Clojure interface for `ramper.util.ByteArrayDiskQueues` for arbitrary data.

   TODO: explanation"
  (:refer-clojure :exclude [count remove])
  (:require [io.pedestal.log :as log]
            [taoensso.nippy :as nippy])
  (:import (java.io Closeable)
           (ramper.util ByteArrayDiskQueues)))

(defrecord DataDiskQueues [disk-queues directory data->bytes bytes->data]
  Closeable
  (close [_this]
    (.close ^ByteArrayDiskQueues disk-queues)))

(defn data-disk-queues
  "Creates a instance of DataDiskQueue in directory `dir`. Optionally one can
  pass `data->bytes`and `bytes->data` for the serialization of the objects.
  By default this is done by `taoensso.nippy/freeze` and `taoensso.nippy/thaw`."
  ([dir] (data-disk-queues dir {:data->bytes nippy/freeze
                                :bytes->data  nippy/thaw}))
  ([dir {:keys [data->bytes bytes->data]}]
   (DataDiskQueues. (ByteArrayDiskQueues. dir) dir data->bytes bytes->data)))

(defn dequeue
  [^DataDiskQueues {:keys [disk-queues bytes->data] :as _data-disk-queues} key]
  (io! (bytes->data (.dequeue ^ByteArrayDiskQueues disk-queues key))))

(defn enqueue
  [^DataDiskQueues {:keys [disk-queues data->bytes] :as _data-disk-queues} key data]
  (io! (.enqueue ^ByteArrayDiskQueues disk-queues key (data->bytes data))))

(defn remove
  [^DataDiskQueues {:keys [disk-queues] :as _data-disk-queues} key]
  (io! (.remove ^ByteArrayDiskQueues disk-queues key)))

(defn count
  [^DataDiskQueues {:keys [disk-queues] :as _data-disk-queues} key]
  (.count ^ByteArrayDiskQueues disk-queues key))

(defn num-keys
  [^DataDiskQueues {:keys [disk-queues] :as _data-disk-queues}]
  (.numKeys ^ByteArrayDiskQueues disk-queues))

(defn collect-if
  "Performs garbage collection if the space used is below `threshold` and tries to achieve space
  usage of `target-ratio`"
  ([^DataDiskQueues data-disk-queues] (collect-if data-disk-queues 0.5 0.75))
  ([^DataDiskQueues {:keys [disk-queues] :as _data-disk-queues} threshold target-ratio]
   (when (< (.ratio ^ByteArrayDiskQueues disk-queues) threshold)
     (io!
      (log/info :workbench-virtualizer "Start collection ...")
      (.collect ^ByteArrayDiskQueues disk-queues target-ratio)
      (log/info :workbench-virtualizer "Completed collection ...")))))
