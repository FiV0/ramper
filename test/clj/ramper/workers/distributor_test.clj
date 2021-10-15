(ns ramper.workers.distributor-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [io.pedestal.log :as log]
            [matcher-combinators.test]
            [ramper.frontier.workbench2 :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.sieve.disk-flow-receiver :as disk-flow-receiver]
            [ramper.sieve.flow-receiver :as flow-receiver]
            [ramper.sieve :as sieve]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.util :as util]
            [ramper.util.byte-serializer :as serializer]
            [ramper.util.url :as url]
            [ramper.workers.distributor :as distributor])
  (:import (java.net InetAddress)))

(defn- hash' [x] (-> x hash long))

(deftest enlarge-front-test
  (testing "enlarge-front by itself"
    (let [urls ["https://clojure.org/about/rationale/" ;; already to many urls
                "https://httpbin.org/get" ;; has a corresponding entry
                "https://foobar.org/" ;; has a corresponding entry that is full
                "https://java.com/"       ;; gets a new entry
                "https://java.com/about/"]
          workbench (atom (-> (workbench/workbench)
                              (workbench/add-entry
                               (assoc (workbench/entry (url/scheme+authority (second urls)))
                                      :ip-address (InetAddress/getByAddress (byte-array 4))))))
          ;; TODO remove 1000
          _ (swap! workbench (fn [wb] (reduce #(workbench/add-path-query %1 (url/scheme+authority (nth urls 2)) %2)
                                              wb
                                              (repeat 1000 "/get"))))
          ;; _ (println @workbench #_(count (workbench/queued-path-queries @workbench (url/scheme+authority (nth urls 2)))))
          ready-urls (disk-flow-receiver/disk-flow-receiver (serializer/string-byte-serializer))
          _ (do
              (flow-receiver/prepare-to-append ready-urls)
              (run! #(flow-receiver/append ready-urls (hash' %) %) urls)
              (flow-receiver/finish-appending ready-urls))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "tmp-virtualizer"))
          scheme+authority-to-count (atom {(url/scheme+authority (first urls)) 3})
          runtime-config (atom {:ramper/max-urls-per-scheme+authority 3})
          new-entries (atom clojure.lang.PersistentQueue/EMPTY)
          thread-data {:workbench workbench :ready-urls ready-urls
                       :virtualizer virtualizer :scheme+authority-to-count scheme+authority-to-count
                       :runtime-config runtime-config :new-entries new-entries}
          stats {:deleted-from-sieve 0
                 :from-sieve-to-virtualizer 0
                 :from-sieve-to-workbench 0}
          new-stats (distributor/enlarge-front thread-data stats)]
      (Thread/sleep 100)
      (is (= {:deleted-from-sieve 1
              :from-sieve-to-virtualizer 1
              :from-sieve-to-workbench 3}
             new-stats))
      (is (= 1 (count @new-entries)))
      (is (= (url/scheme+authority (nth urls 3))
             (:scheme+authority (peek @new-entries))))
      (is (= 2 (count (:path-queries (peek @new-entries)))))
      (is (= 1 (virtual/on-disk virtualizer)))
      (is (= 1 (virtual/count virtualizer (workbench/entry (url/scheme+authority (nth urls 2))))))
      (is (= 3 (count (:base->path-queries @workbench)))))))

(defn- create-dummy-ip [s]
  (let [ba (byte-array 4)]
    (doall (map-indexed #(aset-byte ba %1 %2) s))
    (InetAddress/getByAddress ba)))

(deftest distributor-thread-test
  (testing ""
    (let [urls ["https://clojure.org/about/rationale/" ;; already to many urls
                "https://httpbin.org/get" ;; has a corresponding entry
                "https://foobar.org/" ;; has a corresponding entry that is full
                "https://java.com/"       ;; gets a new entry
                "https://java.com/about/"]
          dummy-ips (->> (range 4)
                         (map #(create-dummy-ip [1 1 1 %])))
          workbench (atom (-> (workbench/workbench)
                              ;; entry that won't get dequeued
                              (workbench/add-entry
                               (assoc (workbench/entry (url/scheme+authority (second urls)))
                                      :ip-address (first dummy-ips)
                                      :next-fetch (util/from-now 100)))
                              ;; entry that will be purged
                              (workbench/add-entry
                               (-> (workbench/entry (url/scheme+authority "https://foo.bar"))
                                   (assoc :ip-address (second dummy-ips)
                                          :next-fetch (util/from-now -3))))
                              ;; entry that won't get purged, but also not refilled
                              (workbench/add-entry
                               (-> (workbench/entry (url/scheme+authority "https://foo.toto") ["/hello/world"])
                                   (assoc :ip-address (nth dummy-ips 2)
                                          :next-fetch (util/from-now -2))))
                              ;; entry that gets refilled in memory
                              (workbench/add-entry
                               (-> (workbench/entry (url/scheme+authority "https://bar.toto"))
                                   (assoc :ip-address (nth dummy-ips 3)
                                          :next-fetch (util/from-now -1))))))
          ;; TODO remove 1000
          _ (swap! workbench (fn [wb] (reduce #(workbench/add-path-query %1 (url/scheme+authority (nth urls 2)) %2)
                                              wb
                                              (repeat 1000 "/get"))))
          ready-urls (disk-flow-receiver/disk-flow-receiver (serializer/string-byte-serializer))
          sieve (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                               32 ready-urls (serializer/string-byte-serializer)
                                               hash')
          _ (do
              (run! #(sieve/enqueue sieve %) urls)
              (sieve/flush sieve))
          refill-queue (atom (into clojure.lang.PersistentQueue/EMPTY (repeatedly 3 #(workbench/dequeue-entry! workbench))))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "tmp-virtualizer"))
          ;; TODO these dummy visit state are awkward
          _ (run! #(virtual/enqueue virtualizer (workbench/entry (url/scheme+authority "https://bar.toto")) %)
                  ["https//bar.toto/hello/world" "https//bar.toto/hello/foo"])
          runtime-config (atom {:ramper/runtime-stop false
                                :ramper/max-urls-per-scheme+authority 3
                                :ramper/workbench-max-byte-size (* 1024 1024)
                                :ramper/ip-delay 2000
                                :ramper/scheme+authority-delay 2000
                                :ramper/required-front-size 1000})
          scheme+authority-to-count (atom {(url/scheme+authority (first urls)) 3})
          new-entries (atom clojure.lang.PersistentQueue/EMPTY)
          path-queries-in-queues (atom 5)
          thread-data {:workbench workbench
                       :refill-queue refill-queue :stats-chan (async/chan (async/sliding-buffer 3))
                       :virtualizer virtualizer :sieve sieve
                       :runtime-config runtime-config :ready-urls ready-urls
                       :scheme+authority-to-count scheme+authority-to-count
                       :new-entries new-entries
                       :path-queries-in-queues path-queries-in-queues}
          thread (async/thread (distributor/distributor-thread thread-data))]
      (Thread/sleep 100)
      (swap! runtime-config assoc :ramper/runtime-stop true)
      (is (true? (async/<!! thread)))
      (is (= 1 (count @new-entries)))
      (is (= (url/scheme+authority (nth urls 3))
             (:scheme+authority (peek @new-entries))))
      (is (= 2 (count (:path-queries (peek @new-entries)))))
      (is (= 1 (virtual/on-disk virtualizer)))
      (is (= 1 (virtual/count virtualizer (workbench/entry (url/scheme+authority (nth urls 2))))))
      (is (= 5 (workbench/nb-workbench-entries @workbench))))))
