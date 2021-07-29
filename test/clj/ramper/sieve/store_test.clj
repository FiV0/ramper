(ns ramper.sieve.store-test
  (:require [clojure.test :refer [deftest testing is]]
            [ramper.sieve.store :as store]
            [ramper.util :as util]))

(deftest store-testing
  (let [s (store/store true (util/temp-dir "sieve") "tmp-sieve" 128)
        start 1000000
        s (testing "creating, writing to new store"
            (let [s (store/open s)]
              (doseq [cur (range start (+ start 512))]
                (store/append s (long cur)))
              (store/close s)))
        _ (is 512 (store/size s))
        s (testing "reading and writing to store"
            (let [s (store/open s)]
              (doseq [cur (range start (+ start 512))]
                (let [cur2 (store/consume s)]
                  (is (= cur cur2))
                  (store/append s (long cur))
                  (store/append s (long cur))))
              (store/close s)))]
    (is 1024 (store/size s))
    (testing "final reading rally"
      (let [s (store/open s)]
        (doseq [_ (range 512)]
          (let [cur (store/consume s)
                cur2 (store/consume s)]
            (is (= cur cur2))))
        (store/close s)))
    (is 1024 (store/size s))))
