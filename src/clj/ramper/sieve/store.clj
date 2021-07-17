(ns ramper.sieve.store
  "A store holds the hashes that have already been processed by the sieve.

  The idea of the store is that hashes are always sorted on disk and that
  consuming and appending can happen at the same time (through two files).
  This allows a linear two pointer approach when adding new hashes to the store."
  (:require [clojure.java.io :as io]
            [ramper.util :as util])
  (:import (java.io FileInputStream FileOutputStream IOException)
           (java.nio ByteBuffer ByteOrder)))

(defrecord Store [name output-file output-buffer input-buffer input-channel output-channel])

(defn- allocate-byte-buffer [size]
  (-> (ByteBuffer/allocateDirect (util/multiple-of-8 size))
      (.order (ByteOrder/nativeOrder))))

(defn store
  "Creates a new store."
  [new sieve-dir sieve-name buffer-size]
  (let [name (io/file sieve-dir sieve-name)]
    (when (and new (not (.createNewFile name))) (throw (IOException. (str "Sieve store " name " exists"))))
    (when (and (not new) (not (.exists name))) (throw (IOException. (str "Sieve store " name " does not exist"))))
    (->Store name
             (io/file sieve-dir (str sieve-name "~"))
             (allocate-byte-buffer buffer-size)
             (allocate-byte-buffer buffer-size)
             nil
             nil)))

(defn open
  "Open the store for consuming and appending."
  [{:keys [name output-file input-buffer output-buffer] :as store}]
  (.clear output-buffer)
  (.clear input-buffer)
  (.flip input-buffer)
  (conj store {:input-channel (-> (FileInputStream. name) (.getChannel))
               :output-channel (-> (FileOutputStream. output-file) (.getChannel))}))

(defn append
  "Append a hash (should be a long) to the store."
  [{:keys [output-buffer output-channel] :as store} hash]
  (io! "`append` of Store called in transaction!"
       (.putLong output-buffer hash)
       (when-not (.hasRemaining output-buffer)
         (.flip output-buffer)
         (.write output-channel output-buffer)
         (.clear output-buffer))
       store))

(defn consume
  "Consume a hash from the store."
  [{:keys [input-buffer input-channel]}]
  (io! "`consume` of Store called in transaction!"
       (when-not (.hasRemaining input-buffer)
         (.clear input-buffer)
         (.read input-channel input-buffer)
         (.flip input-buffer))
       (.getLong input-buffer)))

(defn close
  "Close a store."
  [{:keys [name output-file input-channel output-buffer output-channel] :as store}]
  (.flip output-buffer)
  (.write output-channel output-buffer)
  (.close output-channel)
  (.close input-channel)
  (when-not (.delete name) (throw (IOException. (str "Cannot delete store " name))))
  (when-not (.renameTo output-file name) (throw (IOException. (str "Cannot rename new store file " output-file " to " name))))
  (conj store {:input-channel nil :output-channel nil}))

(defn size
  "The size of the underlying store."
  [{:keys [name]}]
  (/ (.length name) (/ Long/SIZE Byte/SIZE)))

(defn check-store
  "For internal use only!!!"
  [store]
  (let [store (open store)
        s (size store)]
    (loop [res []]
      (if (< (count res) s)
        (recur (conj res (consume store)))
        (when (or (not= res (vec (sort res)))
                  (not= (count res) (count (set res))))
          (throw (IllegalStateException. "Store inconsitent !!!")))))
    (close store)))
