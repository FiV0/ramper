(ns ramper.workers.dns-resolving-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench2 :as workbench]
            [ramper.frontier.workbench.ip-store :as ip-store]
            [ramper.util :as util]
            [ramper.util.delay-queue :as delay-queue]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving])
  (:import (java.util Arrays)
           (org.xbill.DNS Address)))

(deftest global-java-dns-resolver-test
  (testing "global-java-dns-resolver"
    (let [host "httpbin.org"
          dns-resolver (dns-resolving/global-java-dns-resolver)
          address (Address/getByName host)]
      (.addHost dns-resolver host address)
      (is (true? (Arrays/equals (.getAddress address) (-> (.resolve dns-resolver host) first .getAddress))))
      (.deleteHost dns-resolver host))))

(deftest dns-thread-test
  (testing "ramper.worker.dns-resolving/dns-thread"
    (let [dns-resolver (dns-resolving/java-dns-resolver)
          wb (atom (workbench/workbench))
          ip-store (atom (ip-store/ip-store))
          unknown-hosts (atom (delay-queue/delay-queue))
          new-entries (atom clojure.lang.PersistentQueue/EMPTY)
          vs1 (-> (workbench/entry (url/scheme+authority "https://finnvolkel.com"))
                  (assoc :next-fetch (util/from-now 90)))
          vs2-bad (workbench/entry (url/scheme+authority "http://asdf.asdf"))
          vs3 (-> (workbench/entry (url/scheme+authority "https://news.ycombinator.com"))
                  (assoc :next-fetch (util/from-now 100)))
          _ (swap! new-entries into [vs1 vs2-bad vs3])
          arg-map {:dns-resolver dns-resolver
                   :workbench wb
                   :unknown-hosts unknown-hosts
                   :new-entries new-entries
                   :ip-store ip-store}
          tw1 (thread-util/thread-wrapper (partial dns-resolving/dns-thread arg-map 1))
          tw2 (thread-util/thread-wrapper (partial dns-resolving/dns-thread arg-map 2))]
      (Thread/sleep 200)
      (thread-util/stop tw1)
      (thread-util/stop tw2)
      (is (true? (thread-util/stopped? tw1)))
      (is (true? (thread-util/stopped? tw2)))
      (is (= 1 (count @unknown-hosts)))
      (is (= 2 (workbench/nb-workbench-entries @wb)))
      (let [vs1-dequeued (workbench/peek-entry @wb)]
        (is (= "https://finnvolkel.com" (-> vs1-dequeued :scheme+authority str)))
        (is (not (nil? (-> vs1-dequeued :ip-address)))))
      (workbench/dequeue-entry! wb)
      (let [vs3-dequeued (workbench/peek-entry @wb)]
        (is (= "https://news.ycombinator.com" (-> vs3-dequeued :scheme+authority str)))
        (is (not (nil? (-> vs3-dequeued :ip-address))))))))
