(ns ramper.sieve.bucket-test
  (:require [clojure.test :refer [deftest testing is]]
            [ramper.sieve.bucket :as bucket]
            [ramper.util :as util]
            [ramper.util.byte-serializer :as serializer]))

(defn hash' [x] (-> x hash long))

(deftest bucket-testing
  (testing "bucket reading and writing"
    (let [b (-> (bucket/bucket (serializer/long-byte-serializer) 128 32 (util/temp-dir "bucket-test"))
                bucket/prepare)
          start 1000000]

      (loop [cur start]
        (when (< cur (+ start 512))
          (bucket/append b (hash' cur) cur)
          (recur (inc cur))))

      (loop [cur start]
        (when (< cur (+ start 512))
          (if (= 0 (mod cur 2))
            (is cur (bucket/consume-key b))
            (bucket/skip-key b))
          (recur (inc cur))))
      (bucket/close b))))
