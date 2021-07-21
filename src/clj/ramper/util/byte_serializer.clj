(ns ramper.util.byte-serializer
  (:require [ramper.util :as util]
            [taoensso.nippy :as nippy])
  (:import (it.unimi.dsi.fastutil.io FastBufferedInputStream)
           (java.io DataInputStream DataOutputStream EOFException
                    InputStream OutputStream IOException)))

(defn read-int
  "Reads an `java.lang.Integer` from an InputStream `is`."
  [^InputStream is]
  (Integer/valueOf (.readInt (DataInputStream. is))))

(defn read-long
  "Reads a `java.lang.Long` from an InputStream `is`."
  [^InputStream is]
  (Long/valueOf (.readLong (DataInputStream. is))))

(defn read-array
  "Reads an vbyte encoded array from an InputStream `is`."
  [^InputStream is]
  (let [length (util/read-vbyte is)
        ba (byte-array length)
        actual (.read is ba)]
    (when-not (= length actual)
      (throw (IOException. (str "Asked for " length " but got " actual))))
    ba))

(defn write-int
  "Writes an `java.lang.Integer` `x` to an OutputStream `os`."
  [^OutputStream os ^Integer x]
  (.writeInt (DataOutputStream. os) (.intValue x)))

(defn write-long
  "Writes a `java.lang.Long` `x` to an OutputStream `os`."
  [^OutputStream os ^Long x]
  (.writeLong (DataOutputStream. os) (.longValue x)))

(defn write-array
  "Writes a byte array `x` vbyte encoded to an OutputStream `os`."
  [^OutputStream os ^bytes ba]
  (util/write-vbyte os (count ba))
  (.write os ba))

(defn skip-int
  "Skip a `java.lang.Integer` from an FastBufferedInputStream `is`."
  [^FastBufferedInputStream is]
  (.skip is 4))

(defn skip-long
  "Skip a `java.lang.Long` from an FastBufferedInputStream `is`."
  [^FastBufferedInputStream is]
  (.skip is 8))

(defn skip-array
  "Skip a byte array from an FastBufferedInputStream `is`."
  [^FastBufferedInputStream is]
  (loop [length 0]
    (let [b (.read is)]
      (if (>= b 0x80)
        (recur (-> length
                   (bit-or (bit-and b 0x7F))
                   (bit-shift-left 7)))
        (do
          (when (= b -1) (throw (EOFException.)))
          (let [length (bit-or length b)
                actual (.skip is length)]
            (when-not (= length actual)
              (throw (IOException. (str "Asked for " length " but got " actual))))))))))

;; TODO this is broken conceptually

(defprotocol ByteSerializer
  (to-stream [this os x])
  (from-stream [this is])
  (skip [this is]))

(deftype IntByteSerializer []
  ByteSerializer
  (to-stream [_ os x] (write-int os x))
  (from-stream [_ is] (read-int is))
  (skip [_ is] (skip-int is)))

(defn int-byte-serializer []
  (->IntByteSerializer))

(deftype LongByteSerializer []
  ByteSerializer
  (to-stream [_ os x] (write-long os x))
  (from-stream [_ is] (read-long is))
  (skip [_ is] (skip-long is)))

(defn long-byte-serializer []
  (->LongByteSerializer))

(deftype ArrayByteSerializer []
  ByteSerializer
  (to-stream [_ os x] (write-array os x))
  (from-stream [_ is] (read-array is))
  (skip [_ is] (skip-array is)))

(defn array-byte-serializer []
  (->ArrayByteSerializer))

(deftype StringByteSerializer []
  ByteSerializer
  (to-stream [_ os x] (->> x util/string->bytes (write-array os)))
  (from-stream [_ is] (-> (read-array is) util/bytes->string))
  (skip [_ is] (skip-array is)))

(defn string-byte-serializer []
  (->StringByteSerializer))

(deftype DataByteSerializer []
  ByteSerializer
  (to-stream [this os x] (->> x nippy/freeze (write-array os)))
  (from-stream [this is] (-> is read-array nippy/thaw))
  (skip [this is] (-> is skip-array)))

(defn data-byte-serializer []
  (->DataByteSerializer))

(comment
  (require '[clojure.java.io :as io])
  (import '(java.io File))

  (let [tmp-file (File/createTempFile "tmp-" "byte-serializer-test")
        test-int (rand-int 1000)
        test-long (- (long (/ Long/MAX_VALUE 2)) 5)
        test-array (byte-array 3 '(1 2 3))]
    (.deleteOnExit tmp-file)

    (with-open [os (io/output-stream tmp-file)]
      (write-int os test-int)
      (write-array os test-array)
      (write-long os test-long)
      (write-array os test-array))

    (with-open [is (-> tmp-file io/input-stream FastBufferedInputStream.)]
      (assert (= test-int (read-int is)))
      (skip-array is)
      (assert (= test-long (read-long is)))
      (assert (java.util.Arrays/equals test-array (read-array is))))))
