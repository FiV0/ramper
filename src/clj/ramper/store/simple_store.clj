(ns ramper.store.simple-store
  "A simple non parallelized store."
  (:refer-clojure :exclude [read])
  (:require [clojure.java.io :as io]
            [ramper.store :as store :refer [Store StoreReader]]
            [ramper.store.simple-record :as simple-record]
            [ramper.util.byte-serializer :as byte-serializer :refer [to-stream from-stream]])
  (:import (java.io Closeable FileInputStream FileOutputStream IOException)))

;; SimpleStore documentation
;;
;; output-stream - the output stream this store writes to
;; serializer - the serializer object to write to file
;; lock - as one can not write in parallel to the store, it
;;        needs a lock for synchronization

(deftype SimpleStore [output-stream serializer lock]
  Closeable
  (close [_this]
    (.close output-stream))

  Store
  (store [_this url response]
    (io!
     (locking lock
       (to-stream serializer output-stream (simple-record/simple-record url response))))))

(defn simple-store
  "Creates a simple store in directory `dir`."
  ([dir] (simple-store dir true))
  ([dir is-new]
   (let [store-file (io/file dir store/store-name)]
     (cond
       (and is-new (.exists store-file) (not (zero? (.length store-file))))
       (throw (IOException. (str "Store exists and it is not empty, but the crawl"
                                 "is new; it will not be overwritten: " store-file)))

       (and (not is-new) (not (.exists store-file)))
       (throw (IOException. (str "Store does not exist, but the crawl is not "
                                 "new; it will not be created: " store-file)))

       is-new (->SimpleStore (FileOutputStream. store-file)
                             (byte-serializer/data-byte-serializer)
                             (Object.))

       :else (->SimpleStore (FileOutputStream. store-file true)
                            (byte-serializer/data-byte-serializer)
                            (Object.))))))

;; TODO: can this be merged with above?

(deftype SimpleStoreReader [input-stream serializer lock]
  Closeable
  (close [_this]
    (.close input-stream))

  StoreReader
  (read [_this]
    (io!
     (locking lock
       (when (pos? (.available input-stream))
         (from-stream serializer input-stream))))))

(defn simple-store-reader
  "Creates a simple store reader for the directory `dir`."
  ([dir]
   (let [store-file (io/file dir store/store-name)]
     (if (not (.exists store-file))
       (throw (IOException. (str "Store does not exist:" store-file)))

       (->SimpleStoreReader (FileInputStream. store-file) (byte-serializer/data-byte-serializer) (Object.))))))

(comment
  (import '[java.io File])
  (let [tmp-file (File/createTempFile "foo" "bar")
        os (io/output-stream tmp-file)
        is (io/input-stream tmp-file)]
    (.write os (int 15))
    (.close os)
    (println "read:" (.read is))
    (.available is)))
