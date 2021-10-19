(ns ramper.workers.distributor-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [io.pedestal.log :as log]
            [matcher-combinators.test]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.sieve.disk-flow-receiver :as disk-flow-receiver]
            [ramper.sieve.flow-receiver :as flow-receiver]
            [ramper.sieve :as sieve]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.util :as util]
            [ramper.util.byte-serializer :as serializer]
            [ramper.util.url :as url]
            [ramper.workers.distributor :as distributor])
  (:import (java.net InetAddress)
           (ramper.frontier Entry Workbench3)))

(defn- hash' [x] (-> x hash long))

(deftest enlarge-front-test
  (testing "enlarge-front by itself"
    (let [urls ["https://clojure.org/about/rationale/" ;; already to many urls
                "https://httpbin.org/get" ;; has a corresponding entry
                "https://foobar.org/" ;; has a corresponding entry that is full
                "https://java.com/"       ;; gets a new entry
                "https://java.com/about/"]
          workbench (Workbench3.)
          entry-httpbin (.getEntry workbench (str (url/scheme+authority (second urls))))
          _ (.addEntry workbench
                       (doto entry-httpbin
                         (.addPathQuery "/post")
                         (.setIpAddress (InetAddress/getByAddress (byte-array 4)))))
          ;; TODO remove 1000
          entry-foobar (.getEntry workbench (str (url/scheme+authority (nth urls 2))))
          _ (dotimes [_ 1000] (.addPathQuery entry-foobar "/get"))
          _ (.addEntry workbench entry-foobar)
          ready-urls (disk-flow-receiver/disk-flow-receiver (serializer/string-byte-serializer))
          _ (do
              (flow-receiver/prepare-to-append ready-urls)
              (run! #(flow-receiver/append ready-urls (hash' %) %) urls)
              (flow-receiver/finish-appending ready-urls))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "tmp-virtualizer"))
          scheme+authority-to-count (atom {(str (url/scheme+authority (first urls))) 3})
          runtime-config (atom {:ramper/max-urls-per-scheme+authority 3})
          new-entries (atom clojure.lang.PersistentQueue/EMPTY)
          thread-data {:workbench workbench :ready-urls ready-urls
                       :virtualizer virtualizer :scheme+authority-to-count scheme+authority-to-count
                       :runtime-config runtime-config :new-entries new-entries}
          stats {:deleted-from-sieve 0
                 :from-sieve-to-virtualizer 0
                 :from-sieve-to-workbench 0}
          _ (is (= 2 (.numberOfEntries workbench)))
          new-stats (distributor/enlarge-front thread-data stats)]
      (Thread/sleep 100)
      (is (= {:deleted-from-sieve 1
              :from-sieve-to-virtualizer 1
              :from-sieve-to-workbench 3}
             new-stats))
      (is (= 1 (count @new-entries)))
      (is (= (str (url/scheme+authority (nth urls 3)))
             (.-schemeAuthority (peek @new-entries))))
      (is (= 2 (.size (peek @new-entries))))
      (is (= 1 (virtual/on-disk virtualizer)))
      (is (= 1 (virtual/count virtualizer (Entry. (str (url/scheme+authority (nth urls 2)))))))
      (is (= 3 (.numberOfEntries workbench))))))

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
          workbench (Workbench3.)
          entry1 (doto (.getEntry workbench (str (url/scheme+authority (second urls))))
                   (.setIpAddress (first dummy-ips))
                   (.setNextFetch (util/from-now 100)))
          entry2 (doto (.getEntry workbench (str (url/scheme+authority "https://foo.bar")))
                   (.setIpAddress (second dummy-ips))
                   (.setNextFetch (util/from-now -3)))
          entry3 (doto (.getEntry workbench (str (url/scheme+authority "https://foo.toto")))
                   (.addPathQuery "/hello/world")
                   (.setIpAddress (second dummy-ips))
                   (.setNextFetch (util/from-now -2)))
          entry4 (doto (.getEntry workbench (str (url/scheme+authority "https://bar.toto")))
                   (.setIpAddress (nth dummy-ips 3))
                   (.setNextFetch (util/from-now -1)))
          entry5 (doto (.getEntry workbench (str (url/scheme+authority (nth urls 2))))
                   (.setIpAddress (nth dummy-ips 3))
                   (.setNextFetch (util/from-now 200)))
          _ (dotimes [_ 1000] (.addPathQuery entry5 "/get"))
          _ (run! #(.addEntry workbench %) [entry1 entry2 entry3 entry4 entry5])
          ready-urls (disk-flow-receiver/disk-flow-receiver (serializer/string-byte-serializer))
          sieve (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                               32 ready-urls (serializer/string-byte-serializer)
                                               hash')
          _ (do
              (run! #(sieve/enqueue sieve %) urls)
              (sieve/flush sieve))
          refill-queue (atom (into clojure.lang.PersistentQueue/EMPTY (repeatedly 3 #(.popEntry workbench))))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "tmp-virtualizer"))
          ;; TODO these dummy visit state are awkward
          _ (run! #(virtual/enqueue virtualizer (Entry. (str (url/scheme+authority "https://bar.toto"))) %)
                  ["https//bar.toto/hello/world" "https//bar.toto/hello/foo"])
          runtime-config (atom {:ramper/runtime-stop false
                                :ramper/max-urls-per-scheme+authority 3
                                :ramper/workbench-max-byte-size (* 1024 1024)
                                :ramper/ip-delay 2000
                                :ramper/scheme+authority-delay 2000
                                :ramper/required-front-size 1000})
          scheme+authority-to-count (atom {(str (url/scheme+authority (first urls))) 3})
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
      (is (= (str (url/scheme+authority (nth urls 3)))
             (.-schemeAuthority (peek @new-entries))))
      (is (= 2 (.size (peek @new-entries))))
      (is (= 1 (virtual/on-disk virtualizer)))
      (is (= 1 (virtual/count virtualizer (Entry. (str (url/scheme+authority (nth urls 2)))))))
      (is (= 5 (.numberOfEntries workbench))))))
