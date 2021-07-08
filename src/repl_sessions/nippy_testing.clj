(ns repl-sessions.nippy-testing
  (:require [clojure.java.io :as io]
            [ramper.util.byte-serializer :as serializer :refer [ByteSerializer to-stream from-stream skip]]
            [taoensso.nippy :as nippy])
  (:import (java.io File)
           (it.unimi.dsi.fastutil.io FastBufferedInputStream)))

(defrecord QueueRec [queue name])

(defn queue-rec [data name]
  (->QueueRec (into clojure.lang.PersistentQueue/EMPTY data) name))

(deftype QueueRecByteSerializer []
  ByteSerializer
  (to-stream [this os x] (->> x nippy/freeze (serializer/write-array os)))
  (from-stream [this is] (-> is serializer/read-array nippy/thaw))
  (skip [this is] (-> is serializer/skip-array)))

(defn queue-rec-serializer []
  (->QueueRecByteSerializer))


(let [tmp-file (File/createTempFile "tmp-" "nippy-serializer-test")
      data '(1 2 3)
      serializer (queue-rec-serializer)]
  (.deleteOnExit tmp-file)

  (with-open [os (io/output-stream tmp-file)]
    (dotimes [i 5]
      (to-stream serializer os (queue-rec data (str "queue-rec-" i)))))

  (with-open [is (-> tmp-file io/input-stream FastBufferedInputStream.)]
    (loop [res [] i 0]
      (if (< i 5)
        (recur (conj res (from-stream serializer is)) (inc i))
        res))))

(-> *1 (nth 2) :queue seq)
(-> *2 (nth 2) :name )
