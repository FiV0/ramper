(ns ramper.util
  "Utility functions for ramper."
  (:import (java.io InputStream OutputStream)
           (it.unimi.dsi.bits Fast)))

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

(defn string->bytes [s]
  (.getBytes s))

(defn bytes->string [bs]
  (String. bs))

(comment
  (-> "fooüß" string->bytes bytes->string))

(defn multiple-of-8
  "Return the largest multiple of 8 no large than `x`."
  [x]
  (bit-and x (bit-shift-left -1 3)))
