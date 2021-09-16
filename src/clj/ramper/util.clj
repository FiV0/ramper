(ns ramper.util
  "General utility functions for ramper."
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.io InputStream OutputStream)
           (java.nio.file Files)
           (it.unimi.dsi.bits Fast)
           (org.apache.commons.codec.digest MurmurHash3)))

(def runtime (Runtime/getRuntime))

(defn vbyte-length
  "Returns the length of the vByte encoding of the natural number `x`"
  [^Integer x]
  (inc (/ (Fast/mostSignificantBit x) 7)))

(defn write-vbyte
  "Encodes a natural number `x` (Integer) to an OutputStream `os` using vBytes.
  Returns the number of bytes written."
  [^OutputStream os ^Integer x]
  (cond (zero? (bit-shift-right x 7))
        (do (.write os x) 1)

        (zero? (bit-shift-right x 14))
        (do (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            2)

        (zero? (bit-shift-right x 21))
        (do (.write os (bit-or (unsigned-bit-shift-right x 14) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            3)

        (zero? (bit-shift-right x 28))
        (do (.write os (bit-or (unsigned-bit-shift-right x 21) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 14) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            4)

        :else
        (do (.write os (bit-or (unsigned-bit-shift-right x 28) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 21) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 14) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            5)))

(defn read-vbyte
  "Decodes a natural number from an InputStream `is` using vByte."
  [^InputStream is]
  (loop [x 0]
    (let [b (.read is)
          x (bit-or x (bit-and b 0x7F))]
      (if (zero? (bit-and b 0x80))
        x
        (recur (bit-shift-left x 7))))))

(comment
  (require '[clojure.java.io :as io])
  (import '(java.io File))

  (let [tmp-file (File/createTempFile "tmp-" "vbyte-test")
        x-small (rand-int 1000)
        x-large (- (int (/ Integer/MAX_VALUE 2)) 5)]
    (.deleteOnExit tmp-file)

    (with-open [os (io/output-stream tmp-file)]
      (write-vbyte os x-small)
      (write-vbyte os x-large))

    (with-open [is (io/input-stream tmp-file)]
      (assert (= x-small (read-vbyte is)))
      (assert (= x-large (read-vbyte is))))))

;; TODO maybe move string utilities to extra ns

(defn string->bytes [s]
  (.getBytes s))

(defn bytes->string [bs]
  (String. bs))

(comment
  (-> "fooüß" string->bytes bytes->string))

(defn compare-ignore-case [s1 s2]
  (= (str/lower-case s1) (str/lower-case s2)))

(defn multiple-of-8
  "Return the largest multiple of 8 no larger than `x`."
  [x]
  (bit-and x (bit-shift-left -1 3)))

(defn temp-dir
  "Generate a temporary directory in the default temporary-file directory."
  [prefix]
  (-> (Files/createTempDirectory prefix (into-array java.nio.file.attribute.FileAttribute []))
      .toFile))

(comment
  (temp-dir "foo"))

(defn unix-timestamp
  "Returns a unix timestamp for now."
  []
  (long (/ (System/currentTimeMillis) 1000)))

(defn number-of-cores
  "Returns the number of cores available on this machine."
  []
  (.availableProcessors runtime))

(defn rand-str
  "Returns a random string of length `len` in lower"
  [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 97))))))

(defn hash-str
  "Returns a 64 bit hash of `s`"
  [s]
  (-> s hash long))

(defn hash-str-128
  "Returns a 128 bit MurmurHash3 of `s` yielding a vector of two longs."
  [s]
  (vec (MurmurHash3/hash128 s)))

(defn ip-address->str
  "Returns an ip byte array as string."
  [ip-address]
  {:pre [(= 4 (count ip-address))]}
  (->> (map int ip-address)
       (str/join ".")))

(defn InetAddress->str
  "Returns a java.net.InetAddress as string."
  [ip-address]
  (-> (.getAddress ip-address) ip-address->str))

(defn from-now
  "Returns a timestamp `millis` milliseconds from now."
  [millis]
  (+ (System/currentTimeMillis) millis))

(defn project-dir
  "Returns the directory from which the JVM was started."
  []
  (io/file (System/getProperty "user.dir")))

(defn make-absolute
  "Makes a file absolute with respect to the project root."
  [file]
  (let [file (io/file file)]
    (cond->>  file
      (not (.isAbsolute file)) (io/file (project-dir)))))

;; copied from rosetta code
(defn empty-dir?
  "Check if `path` is an empty directory."
  [path]
  (let [file (io/file path)]
    (assert (.exists file))
    (assert (.isDirectory file))
    (-> file .list empty?)))
