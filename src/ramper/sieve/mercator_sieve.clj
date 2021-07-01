(ns ramper.sieve.mercator-sieve
  (:refer-clojure :exclude [flush])
  (:require [io.pedestal.log :as log]
            [ramper.sieve :refer [FlowReceiver prepare-to-append append finish-appending
                                  Sieve enqueue flush
                                  Size number-of-items]
             :as sieve]
            [ramper.sieve.bucket :as bucket]
            [ramper.sieve.store :as store])
  (:import (it.unimi.dsi.fastutil.ints IntArrays)
           (it.unimi.dsi.fastutil.longs LongArrays)))



(defrecord MercatorSieve [receiver serializer hash-function bucket store closed position]
  java.io.Closeable
  (close [sieve]
    (.close bucket)
    (-> (flush sieve) (conj {:closed true})))
  Size
  (number-of-items [this] (number-of-items bucket)))

;; TODO: think about if there should be the possibility of transforming keys before hashing
;; as otherwise stuff might be done multiple times for serialization and hashing

(defn mercator-seive
  "Create a new MercatorSeive.

  The arguments are:
  -`new?` - is this a new or an existing seive.
  -`sieve-dir` - a directory where the sieve should be saved
  -`sieve-size` - the size of the sieve in longs
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
                   (bucket/bucket serializer sieve-size aux-buffer-size sieve-dir)
                   (store/store new sieve-dir "store" store-buffer-size)
                   false
                   (make-array Integer/TYPE sieve-size)))

(defn- fill-array-consecutive
  "Fills array `a` with consecutive number from 0 to `n`-1."
  [a n]
  (loop [i 0]
    (when (< i n)
      (aset a i i)
      (recur (inc i)))))

(extend-type MercatorSieve
  Sieve
  (enqueue [{:keys [closed hash-function bucket] :as sieve} key]
    (when closed (throw (IllegalStateException.)))
    (let [hash (hash-function key)]
      (locking sieve
        (bucket/append bucket hash key)
        (if (bucket/is-full? bucket)
          (do (flush sieve) true)
          false))))

  (flush [{:keys [bucket store position receiver serializer] :as sieve}]
    (locking sieve
      (let [start (System/nanoTime)]
        (if (zero? (number-of-items bucket))
          (throw (IllegalStateException. "No new items in sieve."))
          (let [store (store/open store)
                store-size (store/size store)
                number-of-bucket-items (number-of-items bucket)
                _ (fill-array-consecutive position)
                {:keys [buffer]} bucket]
            (LongArrays/parallelRadixSortIndirect position buffer 0 number-of-bucket-items false)
            (LongArrays/stabilize position buffer 0 number-of-bucket-items)
            (loop [next (if-not (zero? store-size) (.consume store) -1)
                   store-position 0
                   new-hashes 0
                   j 0
                   dups 0]
              (if (< j number-of-bucket-items)
                (let [hash (aget buffer (aget position j))
                      ;; this invalidates duplicates
                      [new-j new-dups] (loop [j j dups dups]
                                         (if (and (< j (dec number-of-bucket-items))
                                                  (= hash (aget buffer (aget position j))))
                                           (do
                                             (aset position (inc j) Integer/MAX_VALUE)
                                             (recur (inc j) (inc dups)))
                                           [j dups]))]
                  (cond
                    ;; no more hashes in the store
                    ;; or the new key comes before the next key in the store
                    (or (= store-position store-size)
                        (< hash next))
                    (do (.append store hash)
                        (recur next store-position (inc new-hashes) new-j new-dups))
                    ;; existing key
                    ;; invalidate position and get next hash from store
                    (= hash next)
                    (do (.append store hash)
                        (aset position j Integer/MAX_VALUE)
                        (recur (if (< store-position (dec store-size)) (.consume store) next)
                               (inc store-position)
                               new-hashes
                               new-j
                               new-dups))
                    ;; old key, just append
                    (< next hash)
                    (do (.append store next)
                        (recur (if (< store-position (dec store-size)) (.consume store) next)
                               (inc store-position)
                               new-hashes
                               new-j
                               new-dups))))
                (do
                  ;; no more hashes in the bucket, but we still write the remaining ones
                  ;; to the new store
                  (loop [store-position store-position next next]
                    (when (< store-position store-size)
                      (.append store next)
                      (recur (inc store-position) (if (< store-position (dec store-size)) (.consume store) next))))
                  (.close store)
                  (log/info :mercator-sort+fusion-completed
                            {:hashes (+ store-size new-hashes)
                             :time (/ (- start (System/nanoTime)) 1e9)}))))

            ;; adding new keys to the FlowReceiver
            (IntArrays/parallelQuickSort position 0 number-of-bucket-items)
            (prepare-to-append receiver)
            (loop [j 0 bucket-position 0]
              (if (and (< j number-of-bucket-items)
                       (not= (aget position j) Integer/MAX_VALUE))
                (let [pos (aget position j)]
                  (if (< bucket-position pos) ;; a duplicate key
                    (do
                      (bucket/skip-key bucket)
                      (recur j (inc bucket-position)))
                    (do
                      (append serializer (aget buffer pos) (bucket/consume-key bucket))
                      (recur (inc j) (inc bucket-position)))))
                ;; here j is actually equal to the number of items added to the bucket
                (let [dups (- number-of-bucket-items j)]
                  (bucket/clear bucket)
                  (finish-appending receiver)
                  (log/info :mercator-end-flow-receiver-appending
                            {:new-keys j
                             :dups dups
                             :unique-key-ratio (* 100.0 (/ dups number-of-bucket-items))}))))
            (let [duration (max (- (System/nanoTime) start) 1)]
              (log/info :mercator-flush-completed
                        {:total-time (/ duration 1e9)}))))))))
