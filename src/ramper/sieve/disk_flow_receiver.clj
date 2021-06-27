(ns ramper.sieve.disk-flow-receiver
  (:refer-clojure :exclude [flush])
  (:require [clojure.java.io :as io]
            [ramper.sieve :refer [FlowReceiver]])
  (:import (java.io DataOutputStream File FileInputStream FileOutputStream)
           (java.util NoSuchElementException)
           (it.unimi.dsi.fastutil.io FastBufferedInputStream FastBufferedOutputStream)))

;; TODO maybe use defrecord here, probably less performant

(defprotocol DiskFlowReceiverDequeue
  (size [this])
  (dequeue-key [this]))

(deftype DiskFlowReceiver [serializer base-name
                           ^:volatile-mutable size ^:volatile-mutable append-size
                           ^:volatile-mutable input ^:volatile-mutable input-index
                           ^:volatile-mutable output ^:volatile-mutable output-index
                           ^:volatile-mutable closed]
  FlowReceiver
  (prepare-to-append [this]
    (locking this
      (when closed (throw (IllegalStateException.)))
      (set! append-size 0)
      (set! output (-> (str (.base-name this) output-index)
                       io/file
                       FileOutputStream.
                       FastBufferedOutputStream.
                       DataOutputStream.))))

  (append [this hash key]
    (locking this
      (when closed (throw (IllegalStateException.)))
      (.writeLong output hash)
      (.. this serializer (toStream output key))
      (set! append-size (inc append-size))))

  (finish-appending [this]
    (locking this
      (when closed (throw (IllegalStateException.)))
      (.close output)
      (let [f (io/file (str (.base-name this) output-index))]
        (if (.length f)
          (.delete f)
          (set! output-index (inc output-index))))
      (set! size (+ size append-size))
      (.notifyAll this)))

  (no-more-append [this]
    (locking this
      (set! closed true)))

  DiskFlowReceiverDequeue
  (size [this]
    (locking this size))

  (dequeue-key [this]
    (locking this
      (when (and closed (zero? size)) (throw NoSuchElementException))
      (while (and (not closed) (zero? size))
        (.wait this)
        (when (and closed (zero? size)) (throw NoSuchElementException)))
      (assert (< 0 size) (str size " <= 0"))
      (while (or (= input-index -1) (zero? (.available input)))
        (when (not= input-index -1)
          (.close input)
          (-> (str base-name input-index) io/file .delete))
        (set! input-index (inc input-index))
        (let [f (-> (str base-name input-index) io/file)]
          (.deleteOnExit f)
          (set! input (-> f FileInputStream. FastBufferedInputStream. DataOutputStream.)))
        (.readLong input) ; discarding hash for here
        (set! size (dec size))
        (.from-steam serializer input)))))

(defn disk-flow-receiver [serializer]
  (->DiskFlowReceiver serializer (File/createTempFile (.getSimpleName DiskFlowReceiver) "-tmp")
                      0 0 nil -1 nil 0 false))
