(ns ramper.workers.parsing-thread-test
  (:require [clj-http.client :as client]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing]]
            [lambdaisland.uri :as uri]
            [matcher-combinators.test]
            [ramper.sieve :as sieve]
            [ramper.sieve.disk-flow-receiver :as receiver]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.store :as store]
            [ramper.store.simple-record :as simple-record]
            [ramper.store.simple-store :as simple-store]
            [ramper.util :as util]
            [ramper.util.lru-immutable :as lru]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.fetched-data :as fetched-data]
            [ramper.workers.parsing-thread :as parsing-thread]))

(defn- select-response-keys [resp] (select-keys resp [:headers :status :body]))

(deftest parse-fetched-data-test
  (let [html-url (uri/uri "https://httpbin.org/html")
        bytes-url (uri/uri "https://httpbin.org/bytes/1000")
        links-url (uri/uri "https://finnvolkel.com/about") ;; TODO use something more persistent
        html-response (client/get (str html-url))
        bytes-response (client/get (str bytes-url))
        links-response (client/get (str links-url))
        html-fetched-data {:url html-url :response html-response}
        bytes-fetched-data {:url bytes-url :response bytes-response}
        links-fetched-data {:url links-url :response links-response}
        store-dir (util/temp-dir "simple-store")
        store (simple-store/simple-store store-dir)
        store-reader (simple-store/simple-store-reader store-dir)
        receiver (receiver/disk-flow-receiver (url/url-byte-serializer))
        sieve (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                             32 receiver (url/url-byte-serializer)
                                             url/hash-url)
        url-cache (lru/create-lru-cache [html-url bytes-url links-url] 100 url/hash-url-128)
        runtime-config (atom {:ramper/max-urls-per-scheme+authority 10})
        scheme+authority-to-count (atom {})
        urls-crawled (atom 0)
        thread-data {:store store :sieve sieve :url-cache url-cache
                     :scheme+authority-to-count  scheme+authority-to-count
                     :runtime-config runtime-config :urls-crawled urls-crawled}]
    (testing "with httpbin.org"
      (is (true? (s/valid? ::fetched-data/fetched-data html-fetched-data)))
      (is (true? (s/valid? ::fetched-data/fetched-data bytes-fetched-data)))
      (parsing-thread/parse-fetched-data (assoc thread-data :fetched-data html-fetched-data))
      (parsing-thread/parse-fetched-data (assoc thread-data :fetched-data bytes-fetched-data))
      (.close store)
      (is (match? (simple-record/simple-record html-url (select-response-keys html-response))
                  (store/read store-reader)))
      (is (match? (simple-record/simple-record bytes-url (select-response-keys bytes-response))
                  (store/read store-reader)))
      (is (= 1 (count @scheme+authority-to-count)))
      ;; two for httpbin.org
      (is (= #{2} (set (vals @scheme+authority-to-count)))))
    (let [store (simple-store/simple-store store-dir false)
          thread-data (assoc thread-data :store store)]
      (testing "with finnvolkel.com and links"
        (is (true? (s/valid? ::fetched-data/fetched-data links-fetched-data)))
        (parsing-thread/parse-fetched-data (assoc thread-data :fetched-data links-fetched-data))
        (.close store)
        (is (match? (simple-record/simple-record links-url (select-response-keys links-response))
                    (store/read store-reader)))
        ;; 3 initial + 4 new
        (is (= 7 (count url-cache)))
        (sieve/flush sieve)
        (is (= '("https://finnvolkel.com/"
                 "https://finnvolkel.com/about/"
                 "https://finnvolkel.com/archive/"
                 "https://nextjournal.com/")
               (repeatedly 4 #(-> (receiver/dequeue-key receiver) str))))
        (is (= 2 (count @scheme+authority-to-count)))
        ;; two for httpbin.org, 1 finnvolkel.com
        (is (= #{1 2} (set (vals @scheme+authority-to-count))))
        (is (= 3 @urls-crawled))
        (.close store-reader)))))

(deftest parsing-thread-test
  (let [html-url (uri/uri "https://httpbin.org/html")
        bytes-url (uri/uri "https://httpbin.org/bytes/1000")
        links-url (uri/uri "https://finnvolkel.com/about")
        html-response (client/get (str html-url))
        bytes-response (client/get (str bytes-url))
        links-response (client/get (str links-url))
        html-fetched-data {:url html-url :response html-response}
        bytes-fetched-data {:url bytes-url :response bytes-response}
        links-fetched-data {:url links-url :response links-response}
        store-dir (util/temp-dir "simple-store")
        store (simple-store/simple-store store-dir)
        store-reader (simple-store/simple-store-reader store-dir)
        receiver (receiver/disk-flow-receiver (url/url-byte-serializer))
        sieve (mercator-sieve/mercator-seive true (util/temp-dir "tmp-sieve") 128 32
                                             32 receiver (url/url-byte-serializer)
                                             url/hash-url)
        url-cache (lru/create-lru-cache [html-url bytes-url links-url] 100 url/hash-url-128)
        runtime-config (atom {:ramper/max-urls-per-scheme+authority 10})
        results-queue (atom (into clojure.lang.PersistentQueue/EMPTY
                                  [html-fetched-data
                                   bytes-fetched-data
                                   links-fetched-data]))
        scheme+authority-to-count (atom {})
        urls-crawled (atom 0)
        thread-data {:store store :sieve sieve :url-cache url-cache
                     :results-queue results-queue :runtime-config runtime-config
                     :scheme+authority-to-count scheme+authority-to-count
                     :urls-crawled urls-crawled :stats-chan (async/chan (async/sliding-buffer 100))}
        tw (thread-util/thread-wrapper (partial parsing-thread/parsing-thread thread-data 1))]
    (testing "parsing-thread"
      (Thread/sleep 1000)
      (thread-util/stop tw)
      (is (true? (thread-util/stopped? tw)))
      (.close store)
      (is (match? (simple-record/simple-record html-url (select-response-keys html-response))
                  (store/read store-reader)))
      (is (match? (simple-record/simple-record bytes-url (select-response-keys bytes-response))
                  (store/read store-reader)))
      (is (match? (simple-record/simple-record links-url (select-response-keys links-response))
                  (store/read store-reader)))
      ;; 3 initial + 4 new
      (is (= 7 (count url-cache)))
      (sieve/flush sieve)
      (is (= '("https://finnvolkel.com/"
               "https://finnvolkel.com/about/"
               "https://finnvolkel.com/archive/"
               "https://nextjournal.com/")
             (repeatedly 4 #(-> (receiver/dequeue-key receiver) str))))
      (is (= 2 (count @scheme+authority-to-count)))
      (is (= #{1 2} (set (vals @scheme+authority-to-count))))
      (is (= 3 @urls-crawled))
      (.close store-reader))))

(comment
  (require '[ramper.util.extraction :as extraction])
  (require '[ramper.util.extraction.jericho])

  (def resp (client/get "https://httpbin.org/bytes/100"))

  (extraction/html->links :jericho (:body resp))
  (extraction/html->links :jericho (slurp "https://finnvolkel.com/about")))
