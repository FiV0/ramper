(ns ramper.sieve.bucket
  "A bucket that holds the current hashes and keys that still need to go through the sieve.
  See also `ramper.sieve.store.`"
  (:require [clojure.java.io :as io]
            [ramper.sieve :refer [Size number-of-items]]
            [ramper.util.byte-serializer :refer [from-stream to-stream skip]])
  (:import (it.unimi.dsi.fastutil.io FastBufferedInputStream FastBufferedOutputStream)
           (java.io FileInputStream FileOutputStream)))

(defrecord Bucket [serializer items size buffer aux-file aux-in aux-out io-buffer]
  Size
  (number-of-items [this] items))

(defn bucket
  "Creates a new bucket."
  [serializer bucket-size buffer-size sieve-dir]
  (let [aux-file (io/file sieve-dir "bucket")
        io-buffer (make-array Byte/TYPE buffer-size)]
    (->Bucket serializer
              0
              bucket-size
              (make-array Long/TYPE bucket-size)
              aux-file
              nil
              (FastBufferedOutputStream. (FileOutputStream. aux-file) io-buffer)
              io-buffer)))

(defn append
  "Add a key to the bucket."
  [{:keys [items size buffer serializer aux-out] :as bucket} hash key]
  {:pre [(< items size) (= java.lang.Long (type hash))]}
  (aset buffer items hash)
  (to-stream serializer aux-out key)
  (conj bucket {:items (inc items)}))

(defn is-full?
  "Check if the bucket is full."
  [{:keys [items size]}]
  (= items size))

(defn prepare
  "Prepare the bucket for consuming."
  [{:keys [aux-out aux-file io-buffer] :as bucket}]
  (.flush aux-out)
  (conj bucket {:aux-in (FastBufferedInputStream. (FileInputStream. aux-file) io-buffer)}))

(defn consume-key
  "Consume a key from the bucket."
  [{:keys [aux-in serializer]}]
  {:pre [(-> aux-in nil? not)]}
  (from-stream serializer aux-in))

(defn skip-key
  "Skips a key from the bucket."
  [{:keys [aux-in serializer]}]
  {:pre [(-> aux-in nil? not)]}
  (skip serializer aux-in))

(defn clear
  "Clear the bucket for reuse."
  [{:keys [aux-out aux-in] :as bucket}]
  (.close aux-in)
  (.position aux-out 0)
  (conj bucket {:items 0 :aux-in nil}))

(defn close
  "Close the bucket."
  [{:keys [aux-file]}]
  (.delete aux-file))

(defn get-buffer
  "Get the buffer of the bucket."
  [{:keys [buffer]}]
  buffer)
