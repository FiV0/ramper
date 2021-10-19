(ns ramper.workers.fetching-thread-test
  (:require [clj-http.conn-mgr :as conn]
            [clj-http.cookies :as cookies]
            [clj-http.core :as core]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing]]
            [matcher-combinators.test]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.util :as util]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving]
            [ramper.workers.fetched-data :as fetched-data]
            [ramper.workers.fetching-thread :as fetching-thread])
  (:import (org.xbill.DNS Address)
           (ramper.frontier Entry Workbench3)))

(deftest fetch-data-test
  (let [ip-address (Address/getByName "httpbin.org")
        runtime-config (atom {:ramper/scheme+authority-delay 2000})
        dns-resolver (dns-resolving/global-java-dns-resolver)
        cookie-store (cookies/cookie-store)
        connection-manager (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        client (core/build-http-client {:http-builder-fns [fetching-thread/set-connection-reuse]}
                                       false connection-manager)
        entry (doto (Entry. "https://httpbin.org")
                (.setIpAddress ip-address)
                (.setLastException Exception)
                (.addPathQuery "/cookies")
                (.addPathQuery "/something/else"))
        entry-400-resp (doto (Entry. "https://httpbin.org")
                         (.setIpAddress ip-address)
                         (.addPathQuery "/status/400")
                         (.addPathQuery "/something/else"))
        bad-entry (doto (Entry. "https://asdf.asdf")
                    (.addPathQuery "/foo/bar")
                    (.addPathQuery "/something/else"))
        fetch-thread-data {:http-client client :dns-resolver dns-resolver
                           :cookie-store cookie-store :runtime-config runtime-config}]
    (testing "simple request with cookies"
      (let [now (System/currentTimeMillis)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry entry))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (.getLastException entry)))
        (is (= "/something/else" (.popPathQuery entry)))
        (is (< now (.getNextFetch entry)))))
    (testing "simple request with expected 400 response"
      (let [now (System/currentTimeMillis)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry entry-400-resp))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (.getLastException entry)))
        (is (= "/something/else" (.popPathQuery entry)))
        (is (< now (.getNextFetch entry)))))
    (testing "simple request with expected error"
      (let [now (System/currentTimeMillis)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry bad-entry))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (true? continue))
        (is (= java.net.UnknownHostException (.getLastException entry)))
        (is (= "/foo/bar" (.popPathQuery entry)))
        (is (< now (.getNextFetch entry)))))
    (testing "simple request with expected error that purges the entry"
      (let [bad-entry (doto bad-entry
                        (.setLastException java.net.UnknownHostException)
                        (.setRetries 1))
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry bad-entry))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (false? continue))
        (is (= 2 (.getRetries entry)))
        (is (= java.net.UnknownHostException (.getLastException entry)))))))

(deftest fetching-thread-test
  (let [runtime-config (atom {:ramper/keepalive-time 5000 ;; this test should not depend on keep alive time
                              :ramper/scheme+authority-delay 2000
                              :ramper/ip-delay 2000
                              :ramper/required-front-size 1000})
        host1 "clojure.org"
        host2 "httpbin.org"
        host3 "finnvolkel.com" ;; explicitly using a wrong ip
        host1-ip (Address/getByName host1)
        host2-ip (Address/getByName host2)
        host3-ip (Address/getByName host3)
        dns-resolver (dns-resolving/global-java-dns-resolver)
        conn-mgr (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        workbench (Workbench3.)
        entry (doto (.getEntry workbench "https://clojure.org")
                (.setIpAddress host1-ip)
                (.setNextFetch (util/from-now -30))
                (.addPathQuery "")
                (.addPathQuery "/about/rationale"))
        entry-with-400-resp (doto (.getEntry workbench "https://httpbin.org")
                              (.setIpAddress host2-ip)
                              (.setNextFetch (util/from-now -20))
                              (.addPathQuery "/status/400")
                              (.addPathQuery "/get"))
        ;; we use a wrong ip to force an certificate error
        bad-entry (doto (.getEntry workbench "https://asdf.asdf")
                    (.setIpAddress host3-ip)
                    (.setNextFetch (util/from-now -10))
                    (.addPathQuery "/foo/bar")
                    (.addPathQuery "/something/else"))
        ;; we let things go through the workbench so bookkeeping in the workbench is correct
        _ (run! #(.addEntry workbench %) [entry entry-with-400-resp bad-entry])
        results-queue (atom clojure.lang.PersistentQueue/EMPTY)
        ;; _ (println (.popEntry workbench))
        todo-queue (atom (into clojure.lang.PersistentQueue/EMPTY (repeatedly 3 #(.popEntry workbench))))
        done-queue (atom clojure.lang.PersistentQueue/EMPTY)
        thread-data {:connection-manager conn-mgr :dns-resolver dns-resolver
                     :workbench workbench :results-queue results-queue
                     :runtime-config runtime-config :todo-queue todo-queue
                     :done-queue done-queue
                     :stats-chan (async/chan (async/sliding-buffer 100))}
        tw (thread-util/thread-wrapper (partial fetching-thread/fetching-thread thread-data 1))]
    (Thread/sleep 2000)
    (thread-util/stop tw)
    (is (true? (thread-util/stopped? tw)))
    (is (empty? @todo-queue))
    (is (= 2 (count @done-queue)))
    (is (= entry (peek @done-queue)))
    (is (= entry-with-400-resp (peek (pop @done-queue))))
    (is (= 4 (count @results-queue)))
    (is (every? #(s/valid? ::fetched-data/fetched-data %) @results-queue))
    (is (match? [200 200 400 200] (map #(-> % :response :status) @results-queue)))))

(deftest fetching-thread-front-expansion
  (let [runtime-config (atom {:ramper/keepalive-time 5000 ;; this test should not depend on keep alive time
                              :ramper/scheme+authority-delay 2000
                              :ramper/ip-delay 2000
                              :ramper/required-front-size 1000})
        dns-resolver (dns-resolving/global-java-dns-resolver)
        conn-mgr (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        workbench (Workbench3.)
        results-queue (atom clojure.lang.PersistentQueue/EMPTY)
        todo-queue (atom clojure.lang.PersistentQueue/EMPTY)
        done-queue (atom clojure.lang.PersistentQueue/EMPTY)
        thread-data {:connection-manager conn-mgr :dns-resolver dns-resolver
                     :workbench workbench :results-queue results-queue
                     :runtime-config runtime-config :todo-queue todo-queue
                     :done-queue done-queue
                     :stats-chan (async/chan (async/sliding-buffer 100))}
        tw (thread-util/thread-wrapper (partial fetching-thread/fetching-thread thread-data 1))]
    (Thread/sleep 500)
    (thread-util/stop tw)
    (is (true? (thread-util/stopped? tw)))
    (is (= (+ 1000 fetching-thread/front-increase) (:ramper/required-front-size @runtime-config)))))
