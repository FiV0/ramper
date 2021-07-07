(ns ramper.sieve.mercator-sieve-test
  (:require
   [io.pedestal.log :as log]
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing run-tests]]
   [ramper.sieve.disk-flow-receiver :as receiver]
   [ramper.sieve :as sieve :refer [no-more-append]]
   [ramper.sieve.mercator-sieve :as mercator-sieve]
   [ramper.util :as util]
   [ramper.util.byte-serializer :as serializer]))

(defn hash' [x] (-> x hash long))

(deftest mercator-sieve-simple-test
  (testing "mercator sieve simple testing"
    (let [r (receiver/disk-flow-receiver (serializer/string-byte-serializer))
          s (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                           32 r (serializer/string-byte-serializer) hash')]
      (run! #(sieve/enqueue s %) ["A0" "A1" "A0" "A2" ])
      (sieve/flush s)
      (is "A0" (receiver/dequeue-key r))
      (is "A1" (receiver/dequeue-key r))
      (is "A2" (receiver/dequeue-key r))
      (run! #(sieve/enqueue s %) ["A5" "A0" "A6" "A1" "A6" "A7"])
      (sieve/flush s)
      (.close s)
      (no-more-append r)
      (is "A5" (receiver/dequeue-key r))
      (is "A6" (receiver/dequeue-key r))
      (is "A7" (receiver/dequeue-key r)))))

(defn- random-string [n]
  (str "A" (rand-int n)))

(deftest mercator-sieve-multi-threaded-sequential-dequeue
  (testing "mercator sieve multithreaed enqueueing, sequential dequeueing"
    (let [r (receiver/disk-flow-receiver (serializer/string-byte-serializer))
          s (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                           32 r (serializer/string-byte-serializer) hash')
          number-items (+ 100 (rand-int 2))
          ;; (rand-int 24)
          items  (repeatedly number-items (partial random-string number-items))
          enqueued (atom #{})
          nb-threads 25
          nb-items-per-thread 25
          threads (repeatedly nb-threads #(future
                                            (dotimes [_ nb-items-per-thread]
                                              (let [item (rand-nth items)]
                                                (sieve/enqueue s item)
                                                (swap! enqueued conj item)))))]
      (run! deref threads)
      (sieve/flush s)
      (let [dequeued (reduce (fn [res _] (conj res (receiver/dequeue-key r)))
                             #{}
                             (range (count @enqueued)))]

        (is (= @enqueued dequeued) "enqueued and dequeued sets must be equal")))))
