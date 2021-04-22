(ns ramper.util.lru
  "A LRU cache implementation using MurmurHash3 128 bit implementation."
  (:import (org.apache.commons.codec.digest MurmurHash3)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.concurrent.atomic AtomicInteger)
           (ramper.util DoublyLinkedList DoublyLinkedList$Node)))

(defn string->bytes [s]
  (bytes (byte-array (map byte s))))

(defrecord MurmurHash [first second])

(defn bytes->murmur-hash [bytes]
  (let [hash-array (MurmurHash3/hash128 bytes)]
    #_(->MurmurHash (first hash-array) (second hash-array))
    (vector (first hash-array) (second hash-array))))

(defprotocol Cache
  (add [this item] "Add an item to the cache")
  (check [this item] "Check whether item is in cache.")
  #_(revoke [this item] "Remove an item from cache."))


(comment
  (def dll (DoublyLinkedList.))
  (def node1 (DoublyLinkedList$Node. dll))
  (def node2 (DoublyLinkedList$Node. dll))

  (= node1 node1)
  (.add dll node1)
  (= node1 (.pop dll))
  (.add dll (DoublyLinkedList$Node. dll 1))

  (def ht (ConcurrentHashMap.))
  (def hash-fn (comp bytes->murmur-hash string->bytes))
  (.put ht (hash-fn "abc") "foo")
  (.get ht (hash-fn "abc"))
  (.get ht (hash-fn "dadsfasf"))

  )

(def lock (atom false))

(defn- cleaning-up [lru-cache]
  (let [dll (.dll lru-cache)]
    (loop [cleanup (.cleanup lru-cache)]
      (when-let [node (first cleanup)]
        (.remove dll node)
        (recur (rest cleanup)))))
  (set! (.cleanup lru-cache) '())
  (set! (.cleanup-counter lru-cache) 0)
  (while (< (.max-fill lru-cache) (.size (.ht lru-cache)))
    (let [dll (.dll lru-cache)]
      (.remove (.ht lru-cache) (.. dll getTail getItem))
      (.pop dll))))

(def ^:dynamic *thread-count* 100)
(def ^:dynamic *cleanup-threshold* 1.0)

(deftype LruCache [max-fill hash-fn dll ht thread-count
                   ^:volatile-mutable cleanup
                   ^:volatile-mutable cleanup-counter]
  Cache

  (add [this item]
    (let [hashed ((.hash-fn this) item)
          new-node (DoublyLinkedList$Node. dll hashed)
          ht (.ht this)
          dll (.dll this)]
      (when-let [old-node (.get ht hashed)]
        (when-not (= old-node (.getHead dll))
          (set! (.cleanup this) (conj (.cleanup this) old-node))
          (set! (.cleanup-counter this) (inc (.cleanup-counter this)))))
      (.put ht hashed new-node)
      (while (not (compare-and-set! lock false true)))
      (.add dll new-node)
      (when (< (+ (.max-fill this) (* *cleanup-threshold* *thread-count*))
               (+ (.size ht) (.cleanup-counter this)))
        (cleaning-up this))
      (reset! lock false)))

  (check [this item]
    (let [hashed ((.hash-fn this) item)]
      (if-let [old-node (.get (.ht this) hashed)]
        (let [dll (.dll this)]
          (if (= old-node (.getHead dll))
            true
            (let [new-node (DoublyLinkedList$Node. dll hashed)]
              (while (not (compare-and-set! lock false true)))
              (.add dll new-node)
              (reset! lock false)
              (set! (.cleanup this) (conj (.cleanup this) old-node))
              (set! (.cleanup-counter this) (inc (.cleanup-counter this)))
              true)))
        false))))

(defn create-lru-cache
  ([max-fill] (create-lru-cache max-fill string->bytes '()))
  ([max-fill hash-fn] (create-lru-cache max-fill hash-fn '()))
  ([max-fill hash-fn data]
   {:pre [(<= max-fill (count data))]}
   (let [dll (DoublyLinkedList.)
         ht (ConcurrentHashMap. max-fill)
         hash-fn (comp bytes->murmur-hash hash-fn)]
     (loop [data data]
       (when-let [item (first data)]
         (let [hashed (hash-fn item)
               node (DoublyLinkedList$Node. dll hashed)]
           (.add dll node)
           (.put ht hashed node))))
     (->LruCache max-fill hash-fn dll ht *thread-count* '() 0))))

(comment
  (MurmurHash3/hash128 (string->bytes "abc"))
  (count (MurmurHash3/hash128 (string->bytes "abc")))
  (first (MurmurHash3/hash128 (string->bytes "abc")))

  )
