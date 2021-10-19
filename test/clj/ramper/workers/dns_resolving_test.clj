(ns ramper.workers.dns-resolving-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.ip-store :as ip-store]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.util :as util]
            [ramper.util.delay-queue :as delay-queue]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving])
  (:import (java.util Arrays)
           (org.xbill.DNS Address)
           (ramper.frontier Workbench3)))

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
          wb (Workbench3.)
          ip-store (atom (ip-store/ip-store))
          unknown-hosts (atom (delay-queue/delay-queue))
          new-entries (atom clojure.lang.PersistentQueue/EMPTY)
          entry1 (doto (.getEntry wb "https://finnvolkel.com")
                   (.setNextFetch (util/from-now 90)))
          entry2-bad (.getEntry wb "http://asdf.asdf")
          entry3 (doto (.getEntry wb "https://news.ycombinator.com")
                   (.setNextFetch (util/from-now 100)))
          _ (swap! new-entries into [entry1 entry2-bad entry3])
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
      (is (= 3 (.numberOfEntries wb)))
      (let [entry1-dequeued (.popEntry wb)]
        (is (= "https://finnvolkel.com" (.-schemeAuthority entry1-dequeued)))
        (is (not (nil? (.getIpAddress entry1-dequeued)))))
      (let [entry3-dequeued (.popEntry wb)]
        (is (= "https://news.ycombinator.com" (.-schemeAuthority entry3-dequeued)))
        (is (not (nil? (.getIpAddress entry3-dequeued))))))))
