(ns ramper.workers.distributor-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [io.pedestal.log :as log]
            [matcher-combinators.test]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.sieve.disk-flow-receiver :as disk-flow-receiver]
            [ramper.sieve.flow-receiver :as flow-receiver]
            [ramper.sieve :as sieve]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.util :as util]
            [ramper.util.byte-serializer :as serializer]
            [ramper.util.url :as url]
            [ramper.workers.distributor :as distributor]))

(defn- hash' [x] (-> x hash long))

(deftest enlarge-front-test
  (testing "enlarge-front by itself"
    (let [urls ["https://clojure.org/about/rationale/" ;; already to many urls
                "https://httpbin.org/get" ;; has a corresponding visit-state
                "https://java.com/"       ;; gets a new visit-state
                "https://java.com/about/"]
          workbench (atom (-> (workbench/workbench)
                              (workbench/add-visit-state
                               (assoc (visit-state/visit-state (url/scheme+authority (second urls)))
                                      :ip-address (byte-array 4)))))
          ready-urls (disk-flow-receiver/disk-flow-receiver (serializer/string-byte-serializer))
          _ (do
              (flow-receiver/prepare-to-append ready-urls)
              (run! #(flow-receiver/append ready-urls (hash' %) %) urls)
              (flow-receiver/finish-appending ready-urls))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "tmp-virtualizer"))
          scheme+authority-to-count (atom {(url/scheme+authority (first urls)) 3})
          runtime-config (atom {:ramper/max-urls-per-scheme+authority 3})
          new-visit-states (atom clojure.lang.PersistentQueue/EMPTY)
          thread-data {:workbench workbench :ready-urls ready-urls
                       :virtualizer virtualizer :scheme+authority-to-count scheme+authority-to-count
                       :runtime-config runtime-config :new-visit-states new-visit-states}
          stats {:deleted-from-sieve 0
                 :from-sieve-to-virtualizer 0
                 :from-sieve-to-workbench 0}
          new-stats (distributor/enlarge-front thread-data stats)]
      (Thread/sleep 100)
      (is (= {:deleted-from-sieve 1
              :from-sieve-to-virtualizer 1
              :from-sieve-to-workbench 2}
             new-stats))
      (is (= 1 (count @new-visit-states)))
      (is (= (url/scheme+authority (nth urls 2))
             (:scheme+authority (peek @new-visit-states))))
      (is (= 2 (count (:path-queries (peek @new-visit-states)))))
      (is (= 1 (virtual/on-disk virtualizer)))
      (is (= 1 (virtual/count virtualizer (visit-state/visit-state (url/scheme+authority (second urls)))))))))

(defn- create-dummy-ip [s]
  (let [ba (byte-array 4)]
    (doall (map-indexed #(aset-byte ba %1 %2) s))
    ba))

(deftest distributor-thread-test
  (testing ""
    (let [urls ["https://clojure.org/about/rationale/" ;; already to many urls
                "https://httpbin.org/get" ;; has a corresponding visit-state
                "https://java.com/"       ;; gets a new visit-state
                "https://java.com/about/"]
          dummy-ips (->> (range 4)
                         (map #(create-dummy-ip [1 1 1 %])))
          workbench (atom (-> (workbench/workbench)
                              ;; visit-state that won't get dequeued
                              (workbench/add-visit-state
                               (assoc (visit-state/visit-state (url/scheme+authority (second urls)))
                                      :ip-address (first dummy-ips)
                                      :next-fetch (util/from-now 100)))
                              ;; visit-state that will be purged
                              (workbench/add-visit-state
                               (-> (visit-state/visit-state (url/scheme+authority "https://foo.bar"))
                                   (assoc :ip-address (second dummy-ips)
                                          :next-fetch (util/from-now -3))))
                              ;; visit-state that won't get purged, but also not refilled
                              (workbench/add-visit-state
                               (-> (visit-state/visit-state (url/scheme+authority "https://foo.toto"))
                                   (visit-state/enqueue-path-query "/hello/world")
                                   (assoc :ip-address (nth dummy-ips 2)
                                          :next-fetch (util/from-now -2))))
                              ;; visit-state that gets refilled
                              (workbench/add-visit-state
                               (-> (visit-state/visit-state (url/scheme+authority "https://bar.toto"))
                                   (assoc :ip-address (nth dummy-ips 3)
                                          :next-fetch (util/from-now -1))))))
          ready-urls (disk-flow-receiver/disk-flow-receiver (serializer/string-byte-serializer))
          sieve (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                               32 ready-urls (serializer/string-byte-serializer)
                                               hash')
          _ (do
              (run! #(sieve/enqueue sieve %) urls)
              (sieve/flush sieve))
          todo-queue (atom clojure.lang.PersistentQueue/EMPTY)
          refill-queue (atom (into clojure.lang.PersistentQueue/EMPTY (repeatedly 3 #(workbench/dequeue-visit-state! workbench))))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "tmp-virtualizer"))
          ;; TODO these dummy visit state are awkward
          _ (run! #(virtual/enqueue virtualizer (visit-state/visit-state (url/scheme+authority "https://bar.toto")) %)
                  ["https//bar.toto/hello/world" "https//bar.toto/hello/foo"])
          runtime-config (atom {:ramper/runtime-stop false
                                :ramper/max-urls-per-scheme+authority 3
                                :ramper/workbench-max-byte-size (* 1024 1024)
                                :ramper/ip-delay 2000
                                :ramper/scheme+authority-delay 2000
                                :ramper/required-front-size 1000})
          scheme+authority-to-count (atom {(url/scheme+authority (first urls)) 3})
          new-visit-states (atom clojure.lang.PersistentQueue/EMPTY)
          path-queries-in-queues (atom 5)
          thread-data {:workbench workbench :todo-queue todo-queue
                       :refill-queue refill-queue :stats-chan (async/chan (async/sliding-buffer 3))
                       :virtualizer virtualizer :sieve sieve
                       :runtime-config runtime-config :ready-urls ready-urls
                       :scheme+authority-to-count scheme+authority-to-count
                       :new-visit-states new-visit-states
                       :path-queries-in-queues path-queries-in-queues}
          thread (async/thread (distributor/distributor-thread thread-data))]
      (Thread/sleep 100)
      (swap! runtime-config assoc :ramper/runtime-stop true)
      (is (true? (async/<!! thread)))
      (is (= 1 (count @new-visit-states)))
      (is (= (url/scheme+authority (nth urls 2))
             (:scheme+authority (peek @new-visit-states))))
      (is (= 2 (count (:path-queries (peek @new-visit-states)))))
      (is (= 1 (virtual/on-disk virtualizer)))
      (is (= 1 (virtual/count virtualizer (visit-state/visit-state (url/scheme+authority (second urls))))))
      (is (= 3 (count (:address-to-entry @workbench))))
      (is (contains? (:address-to-entry @workbench) (workbench/hash-ip (nth dummy-ips 2))))
      (is (contains? (:address-to-entry @workbench) (workbench/hash-ip (nth dummy-ips 3)))))))
