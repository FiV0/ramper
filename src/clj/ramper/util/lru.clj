(ns ramper.util.lru
  "A LRU cache implementation using MurmurHash3 128 bit implementation.

  IMPORTANT!!! This is broken and should not be used except for the Cache
  protocol definition."
  (:require [ramper.util :as util])
  (:import (org.apache.commons.codec.digest MurmurHash3)
           (java.util.concurrent ConcurrentHashMap)
           (ramper.util DoublyLinkedList DoublyLinkedList$Node)))

(defrecord MurmurHash [first second])

;; TODO check if the array could not be used by itself
(defn bytes->murmur-hash [bytes]
  (let [hash-array (MurmurHash3/hash128 ^bytes bytes)]
    #_(->MurmurHash (first hash-array) (second hash-array))
    (vector (first hash-array) (second hash-array))))

(comment
  (MurmurHash3/hash128 (util/string->bytes "abc"))
  (count (MurmurHash3/hash128 (util/string->bytes "abc")))
  (first (MurmurHash3/hash128 (util/string->bytes "abc")))
  )

(defprotocol Cache
  (add [this item] "Add an item to the cache")
  (check [this item] "Check whether item is in cache.")
  #_(revoke [this item] "Remove an item from cache."))

(comment
  (def dll (DoublyLinkedList.))
  (def node1 (DoublyLinkedList$Node. 1))
  (def node2 (DoublyLinkedList$Node. 2))

  (= node1 node1)
  (.add dll node1)
  (= node1 (.pop dll))
  (.add dll (DoublyLinkedList$Node. 1))

  (def ht (ConcurrentHashMap.))
  (def hash-fn (comp bytes->murmur-hash util/string->bytes))
  (.put ht (hash-fn "abc") "foo")
  (.get ht (hash-fn "abc"))
  (.get ht (hash-fn "dadsfasf"))

  )

(defprotocol Cleanup
  (getCleanup [this])
  (setCleanup [this x])
  (getCleanupCounter [this])
  (setCleanupCounter [this x])
  (clearCleanup [this]))

;; IMPORTANT !!! this function should be called by only 1 thread
(defn- cleaning-up [lru-cache]
  (let [dll (.dll lru-cache)]
    (loop [cleanup (getCleanup lru-cache)]
      (when-let [node (first cleanup)]
        (.remove dll node)
        (recur (rest cleanup)))))
  (clearCleanup lru-cache)
  (let [dll (.dll lru-cache)]
    (while (< (.max-fill lru-cache) (.size (.ht lru-cache)))
      (.remove (.ht lru-cache) (.. dll pop getItem)))))

(def ^:dynamic *cleanup-threshold* 1.0)

(comment
  (require '[taoensso.tufte :as tufte]))

(defn- offer [lru-cache hashed]
  (let [ht (.ht lru-cache)
        dll (.dll lru-cache)
        new-node (DoublyLinkedList$Node. hashed)]
    ;; this lock might be avoided
    ;; (while (not (compare-and-set! (.lock lru-cache) false true)))
    (.add dll new-node)
    ;; (reset! (.lock lru-cache) false)
    (when-let [old-node (.put ht hashed new-node)]
      (setCleanup lru-cache (conj (getCleanup lru-cache) old-node))
      (setCleanupCounter lru-cache (inc (getCleanupCounter lru-cache))))))

;; TODO  replace the explicit lock with locking
(deftype LruCache [max-fill hash-fn dll ht thread-count
                   ^:volatile-mutable cleanup
                   ^:volatile-mutable cleanup-counter
                   lock]
  Cleanup
  (getCleanup [this] cleanup)
  (setCleanup [this x] (set! cleanup x))
  (getCleanupCounter [this] cleanup-counter)
  (setCleanupCounter [this x] (set! cleanup-counter x))
  (clearCleanup [this]
    (set! cleanup '())
    (set! cleanup-counter 0))

  Cache
  (add [this item]
    (let [hashed ((.hash-fn this) item)]
      (offer this hashed)
      (when (and (< (+ (.max-fill this) (* *cleanup-threshold* (.thread-count this)))
                    (+ (.size ht) (.cleanup-counter this)))
                 (compare-and-set! (.lock this) false true)) ;; important for this to work
        (cleaning-up this)
        (reset! (.lock this) false)))
    nil)
  (check [this item]
    (let [hashed ((.hash-fn this) item)]
      (if (.containsKey (.ht this) hashed)
        (do
          (offer this hashed)
          true)
        false))))

;; TODO use ConcurrentHashMap.KeySetView here instead of the map interface
(defn create-lru-cache
  ([max-fill] (create-lru-cache max-fill util/string->bytes))
  ([max-fill hash-fn] (create-lru-cache max-fill hash-fn (* 3 (util/number-of-cores))))
  ([max-fill hash-fn thread-count] (create-lru-cache max-fill hash-fn thread-count '()))
  ([max-fill hash-fn thread-count data]
   (let [dll (DoublyLinkedList.)
         ht (ConcurrentHashMap. max-fill)
         hash-fn (comp bytes->murmur-hash hash-fn)]
     (loop [data data]
       (when-let [item (first data)]
         (let [hashed (hash-fn item)
               node (DoublyLinkedList$Node. hashed)]
           (.add dll node)
           (.put ht hashed node))))
     (->LruCache max-fill hash-fn dll ht thread-count '() 0 (atom false)))))

(comment
  (def cache (create-lru-cache 2 util/string->bytes 1))

  (add cache "abc")
  (check cache "abc")
  (check cache "foo")
  (add cache "foo")
  (add cache "bar")
  (check cache "abc")
  )
