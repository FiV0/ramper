(ns ramper.sieve.disk-flow-receiver
  (:refer-clojure :exclude [flush])
  (:require [clojure.java.io :as io]
            [ramper.sieve.flow-receiver :refer [FlowReceiver]]
            [ramper.util.byte-serializer :refer [from-stream to-stream]])
  (:import (java.io DataInputStream DataOutputStream File FileInputStream FileOutputStream)
           (java.util NoSuchElementException)
           (it.unimi.dsi.fastutil.io FastBufferedInputStream FastBufferedOutputStream)))

;; TODO maybe use defrecord here, probably less performant

(defprotocol DiskFlowReceiverDequeue
  (size [this])
  (dequeue-key [this]))

;; TODO maybe be add IllegalStateException also when output or input are not set
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
      (set! output (-> (str base-name output-index)
                       io/file
                       FileOutputStream.
                       FastBufferedOutputStream.
                       DataOutputStream.))))

  (append [this hash key]
    (io! "`append` of DiskFlowReceiver called in transaction!"
         (locking this
           (when closed (throw (IllegalStateException.)))
           (.writeLong ^DataOutputStream output hash)
           (to-stream serializer output key)
           (set! append-size (inc append-size)))))

  (finish-appending [this]
    (locking this
      (when closed (throw (IllegalStateException.)))
      (.close ^DataOutputStream output)
      (let [f (io/file (str base-name output-index))]
        (if (zero? (.length f))
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
    (io! "`dequeue-key` of DiskFlowReceiver called in transaction!"
         (locking this
           (when (and closed (zero? size)) (throw NoSuchElementException))
           (while (and (not closed) (zero? size))
             (.wait this)
             (when (and closed (zero? size)) (throw NoSuchElementException)))
           (assert (< 0 size) (str size " <= 0"))
           (while (or (= input-index -1) (zero? (.available ^DataInputStream input)))
             (when (not= input-index -1)
               (.close ^DataInputStream input)
               (-> (str base-name input-index) io/file .delete))
             (set! input-index (inc input-index))
             (let [f (-> (str base-name input-index) io/file)]
               (.deleteOnExit f)
               (set! input (-> f FileInputStream. FastBufferedInputStream. DataInputStream.))))
           (.readLong ^DataInputStream input) ; discarding hash for now
           (set! size (dec size))
           (from-stream serializer input)))))

(defn disk-flow-receiver [serializer]
  (->DiskFlowReceiver serializer (File/createTempFile (.getSimpleName DiskFlowReceiver) "-tmp")
                      0 0 nil -1 nil 0 false))

(comment
  (require '[clojure.core.async :as async])
  (require '[ramper.util.byte-serializer :as serializer])
  (require '[ramper.util :as util])

  (def receiver (disk-flow-receiver (serializer/->ArrayByteSerializer)))

  (prepare-to-append receiver)

  (loop [i 0]
    (when (< i 1000)
      (let [object (util/string->bytes (str "the ultimate number " i))]
        (append receiver (hash object) object))
      (recur (inc i))))

  (finish-appending receiver)

  (size receiver)

  (dotimes [n 2]
    (async/thread
      (loop []
        (when-not (zero? (size receiver))
          (println "Thread " n " dequeued: " (util/bytes->string (dequeue-key receiver)))
          (recur))))))
