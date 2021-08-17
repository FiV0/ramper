(ns ramper.workers.distributor-test
  (:require [clojure.test :refer [deftest is testing]]
            [io.pedestal.log :as log]
            [matcher-combinators.test]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.sieve.disk-flow-receiver :as disk-flow-receiver]
            [ramper.sieve.flow-receiver :as flow-receiver]
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


#_(deftest distributor-thread-test
    (let [sieve (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                               32 ready-urls (serializer/string-byte-serializer)
                                               )]
      ))
