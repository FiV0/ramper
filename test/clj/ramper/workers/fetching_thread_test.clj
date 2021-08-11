(ns ramper.workers.fetching-thread-test
  (:require [clj-http.client :as client]
            [clj-http.conn-mgr :as conn]
            [clj-http.cookies :as cookies]
            [clj-http.core :as core]
            [clojure.test :refer [deftest is testing]]
            [clojure.spec.alpha :as s]
            [ramper.workers.fetched-data :as fetched-data]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.util :as util]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving]
            [ramper.workers.fetching-thread :as fetching-thread])
  (:import (org.xbill.DNS Address)))

(require '[io.pedestal.log :as log])

(deftest fetch-data-test
  (let [ip-address (.getAddress (Address/getByName "httpbin.org"))
        runtime-config (atom {:ramper/scheme+authority-delay 2000})
        host-map (atom {})
        dns-resolver (dns-resolving/global-java-dns-resolver host-map)
        cookie-store (cookies/cookie-store)
        connection-manager (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        client (core/build-http-client {:http-builder-fns [fetching-thread/set-connection-reuse]}
                                       false connection-manager)
        visit-state (-> (visit-state/visit-state (url/scheme+authority "https://httpbin.org"))
                        (assoc :ip-address ip-address
                               :last-exception Exception)
                        (visit-state/enqueue-path-query "/cookies")
                        (visit-state/enqueue-path-query "/something/else"))
        visit-state-400-resp (-> (visit-state/visit-state (url/scheme+authority "https://httpbin.org"))
                                 (assoc :ip-address ip-address)
                                 (visit-state/enqueue-path-query "/status/400")
                                 (visit-state/enqueue-path-query "/something/else"))
        bad-visit-state (-> (visit-state/visit-state (url/scheme+authority "https://asdf.asdf"))
                            (assoc :ip-address ip-address)
                            (visit-state/enqueue-path-query "/foo/bar")
                            (visit-state/enqueue-path-query "/something/else"))
        fetch-thread-data {:http-client client :host-map host-map
                           :cookie-store cookie-store :runtime-config runtime-config}]
    (testing "simple request with cookies"
      (let [now (System/currentTimeMillis)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state visit-state))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (:last-exception visit-state)))
        (is (= "/something/else" (visit-state/first-path visit-state)))
        (is (< now (:next-fetch visit-state)))))
    (testing "simple request with expected 400 response"
      (let [now (System/currentTimeMillis)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state visit-state-400-resp))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (:last-exception visit-state)))
        (is (= "/something/else" (visit-state/first-path visit-state)))
        (is (< now (:next-fetch visit-state)))))
    (testing "simple request with expected error"
      (let [now (System/currentTimeMillis)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state bad-visit-state))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (true? continue))
        (is (= java.net.UnknownHostException (:last-exception visit-state)))
        (is (= "/foo/bar" (visit-state/first-path visit-state)))
        (is (< now (:next-fetch visit-state)))))
    (testing "simple request with expected error that purges the visit-state"
      (let [bad-visit-state (assoc bad-visit-state :last-exception java.net.UnknownHostException :retries 1)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state bad-visit-state))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (false? continue))
        (is (= 2 (:retries visit-state)))
        (is (= java.net.UnknownHostException (:last-exception visit-state)))))))
