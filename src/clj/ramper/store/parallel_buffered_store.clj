(ns ramper.store.parallel-buffered-store
  "A parallel SimpleRecord writer."
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [ramper.store :as store :refer [Store]]
            [ramper.store.simple-record :as simple-record]
            [ramper.util :as util]
            [ramper.util.byte-serializer :as byte-serializer :refer [to-stream]])
  (:import (it.unimi.dsi.fastutil.io FastBufferedOutputStream FastByteArrayOutputStream)
           (java.io Closeable IOException FileOutputStream)))

(def ^:private output-stream-buffer-size (* 1024 1024))

;; buffers are FastByteArrayOutputStream that are reused as buffers

(defn- flushing-thread [output-stream empty-buffers-ch filled-buffers-ch finished-ch]
  (loop [flushing-exception false]
    (let [buffer (async/<!! filled-buffers-ch)]
      (if (= buffer :ramper.store/finish-flush)
        (async/>!! finished-ch flushing-exception)
        (let [new-flush-exception
              (try
                (.write ^FastBufferedOutputStream output-stream
                        (.-array ^FastByteArrayOutputStream buffer)
                        0
                        (.-length ^FastByteArrayOutputStream buffer))
                false
                (catch Exception e
                  (if (instance? IOException e) e (IOException. e))))]
          (async/>!! empty-buffers-ch buffer)
          (recur new-flush-exception))))))

;; this is a one off object. It can not be reused

(defrecord ParallelBufferedStore [output-stream empty-buffers-ch filled-buffers-ch finished-ch serializer]
  Closeable
  (close [_this]
    (async/>!! filled-buffers-ch :ramper.store/finish-flush)
    (when-let [ex (async/<!! finished-ch)]
      (throw ex))
    (.close ^FastBufferedOutputStream output-stream)
    (async/close! empty-buffers-ch)
    (async/close! filled-buffers-ch)
    (async/close! finished-ch))

  Store
  (store [_this url response]
    (let [buffer (async/<!! empty-buffers-ch)]
      (.reset ^FastByteArrayOutputStream buffer)
      (to-stream serializer buffer (simple-record/simple-record url response))
      (async/>!! filled-buffers-ch buffer))))


(defn parallel-buffered-store
  "Creates a ParallelBufferedStore in directory `dir`."
  ([dir] (parallel-buffered-store dir true))
  ([dir is-new] (parallel-buffered-store dir is-new (* 2 (util/number-of-cores))))
  ([dir is-new buffer-size]
   (let [store-file (io/file dir store/store-name)]
     (cond
       (and is-new (.exists store-file) (not (zero? (.length store-file))))
       ;; TODO readd this when finished testing
       #_(throw (IOException. (str "Store exists and it is not empty, but the crawl"
                                   "is new; it will not be overwritten: " store-file)))
       (do (.delete store-file) (parallel-buffered-store dir is-new buffer-size))

       (and (not is-new) (not (.exists store-file)))
       (throw (IOException. (str "Store does not exist, but the crawl is not "
                                 "new; it will not be created: " store-file)))

       :else
       (let [output-stream (-> (if is-new
                                 (FileOutputStream. store-file)
                                 (FileOutputStream. store-file true))
                               (FastBufferedOutputStream. ^int output-stream-buffer-size))
             empty-buffers-ch (async/chan buffer-size)
             filled-buffers-ch (async/chan buffer-size)
             finished-ch (async/chan 1)]
         ;; creating the buffers
         (async/go
           (dotimes [_ buffer-size]
             (async/>! empty-buffers-ch (FastByteArrayOutputStream.))))
         ;; sending off the flushing thread
         (async/go
           (let [t (Thread. (fn [] (flushing-thread output-stream empty-buffers-ch filled-buffers-ch finished-ch)))]
             (.setName t "ramper.store.parallel-buffered-store:flushing-thread")
             (.start t)))
         (->ParallelBufferedStore output-stream
                                  empty-buffers-ch
                                  filled-buffers-ch
                                  finished-ch
                                  (byte-serializer/data-byte-serializer)))))))
