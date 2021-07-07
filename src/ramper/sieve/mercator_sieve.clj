(ns ramper.sieve.mercator-sieve
  (:refer-clojure :exclude [flush])
  (:require [io.pedestal.log :as log]
            [ramper.sieve :refer [FlowReceiver prepare-to-append append finish-appending
                                  Sieve enqueue flush
                                  Size number-of-items]
             :as sieve]
            [ramper.sieve.bucket :as bucket-api]
            [ramper.sieve.store :as store-api])
  (:import (it.unimi.dsi.fastutil.ints IntArrays)
           (it.unimi.dsi.fastutil.longs LongArrays)))

(defn- fill-array-consecutive
  "Fills array `a` with consecutive number from 0 to `n`-1."
  [^ints a n]
  {:pre [(<= n (count a))]}
  (loop [i 0]
    (when (< i n)
      (aset a i i)
      (recur (inc i)))))

(deftype MercatorSieve [receiver serializer hash-function ^:volatile-mutable bucket
                        ^:volatile-mutable store ^:volatile-mutable closed position]
  java.io.Closeable
  (close [sieve]
    (locking sieve
      (bucket-api/close bucket)
      (flush sieve)
      (set! closed true)))

  Size
  (number-of-items [this] (number-of-items bucket))

  Sieve
  (enqueue [sieve key]
    (when closed (throw (IllegalStateException.)))
    (let [hash (hash-function key)]
      (locking sieve
        (set! bucket (bucket-api/append bucket hash key))
        (when (bucket-api/is-full? bucket)
          (flush sieve)))))

  (flush [sieve]
    (locking sieve
      (let [start (System/nanoTime)]
        (if (zero? (number-of-items bucket))
          (do
            (log/info :mercator-flush-empty {})
            sieve)
          (do
            (set! store (store-api/open store))
            (set! bucket (bucket-api/prepare bucket))
            (let [store-size (store-api/size store)
                  number-of-bucket-items (number-of-items bucket)
                  _ (fill-array-consecutive position number-of-bucket-items)
                  buffer (bucket-api/get-buffer bucket)]
              (LongArrays/parallelRadixSortIndirect position buffer 0 number-of-bucket-items false)
              (LongArrays/stabilize position buffer 0 number-of-bucket-items)
              (loop [next (if-not (zero? store-size) (store-api/consume store) nil)
                     store-position 0
                     new-hashes 0
                     j 0
                     dups 0
                     last-hash nil]
                (if (< j number-of-bucket-items)
                  (let [hash (aget buffer (aget position j))]
                    (cond
                      ;; a duplicate in the bucket, invalidate it
                      (= hash last-hash)
                      (do (aset position j Integer/MAX_VALUE)
                          (recur next store-position new-hashes (inc j) (inc dups) hash))
                      ;; no more hashes in the store
                      ;; or the new key comes before the next key in the store
                      (or (= store-position store-size)
                          (< hash next))
                      (do (store-api/append store hash)
                          (recur next store-position (inc new-hashes) (inc j) dups hash))
                      ;; existing key
                      ;; invalidate position and get next hash from store
                      (= hash next)
                      (do (store-api/append store hash)
                          (aset position j Integer/MAX_VALUE)
                          (recur (if (< store-position (dec store-size)) (store-api/consume store) nil)
                                 (inc store-position)
                                 new-hashes
                                 (inc j)
                                 dups
                                 hash))
                      ;; old key, just append, i.e (< next hash)
                      :else
                      (do (store-api/append store next)
                          (recur (if (< (inc store-position) store-size) (store-api/consume store) nil)
                                 (inc store-position)
                                 new-hashes
                                 j
                                 dups
                                 nil))))
                  (do
                    ;; no more hashes in the bucket, but we still write the remaining ones
                    ;; to the new store
                    (loop [store-position store-position next next]
                      (when (< store-position store-size)
                        (store-api/append store next)
                        (recur (inc store-position) (if (< store-position (dec store-size)) (store-api/consume store) next))))
                    (log/info :mercator-sort+fusion-completed
                              {:hashes (+ store-size new-hashes)
                               :new-hashes new-hashes
                               :unique-key-ration (- 100.0 (* 100.0 (/ dups number-of-bucket-items)))
                               :time (/ (- (System/nanoTime) start) 1e9)}))))

              ;; adding new keys to the FlowReceiver
              (IntArrays/parallelQuickSort position 0 number-of-bucket-items)
              (prepare-to-append receiver)
              (loop [j 0 bucket-position 0]
                (if (and (< j number-of-bucket-items)
                         (not= (aget position j) Integer/MAX_VALUE))
                  (let [pos (aget position j)]
                    (if (< bucket-position pos) ;; a duplicate key
                      (do
                        (bucket-api/skip-key bucket)
                        (recur j (inc bucket-position)))
                      (do
                        (append receiver (aget buffer pos) (bucket-api/consume-key bucket))
                        (recur (inc j) (inc bucket-position)))))
                  ;; here j is actually equal to the number of items that made it through the sieve
                  (let [dups (- number-of-bucket-items (inc j))]
                    (set! bucket (bucket-api/clear bucket))
                    (finish-appending receiver)
                    (log/info :mercator-end-flow-receiver-appending
                              {:new-keys j
                               :dups dups
                               :throughput-ratio (- 100.0 (* 100.0 (/ dups number-of-bucket-items)))}))))

              (let [duration (max (- (System/nanoTime) start) 1)]
                (log/info :mercator-flush-completed
                          {:total-time (/ duration 1e9)})
                (set! store (store-api/close store))))))))))

;; TODO: think about if there should be the possibility of transforming keys before hashing
;; as otherwise stuff might be done multiple times for serialization and hashing

(defn mercator-seive
  "Create a new MercatorSeive.

  The arguments are:
  -`new?` - is this a new or an existing seive.
  -`sieve-dir` - a directory where the sieve should be saved
  -`sieve-size` - the size of the sieve in longs (before a flush is needed)
  -`store-buffer-size` - the size of the buffer in bytes used during seive flushs to read and
  write hashes (allocated twice during flushes)
  -`aux-buffer-size` - the size of the buffer to read and write an auxiliary file
  (always allocated)
  -`receiver` - a receiver implementing the FlowReceiver protocol that receives the
  keys that make it through the seive.
  -`serializer` - a serializer implementing the ByteSerializer protocol and encoding
  the keys that this seive will receive
  -`hash-function` - a hash function to calculate hash of a key (should return a long)"
  [new sieve-dir sieve-size store-buffer-size aux-buffer-size receiver serializer hash-function]
  (log/info :new-mercator-seive {:sieve-size sieve-size
                                 :store-buffer-size store-buffer-size
                                 :aux-buffer-size aux-buffer-size})
  (->MercatorSieve receiver serializer hash-function
                   (bucket-api/bucket serializer sieve-size aux-buffer-size sieve-dir)
                   (store-api/store new sieve-dir "store" store-buffer-size)
                   false
                   (make-array Integer/TYPE sieve-size)))
