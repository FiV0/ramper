(ns ramper.workers.parsing-thread
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.util.url :as url]
            [ramper.util.persistent-queue :as pq]
            [ramper.util.extraction :as extraction]
            [ramper.util.extraction.jericho]
            [ramper.sieve :as sieve]
            [ramper.store :as store]
            [ramper.util.lru :as lru]
            [ramper.util.thread :as thread-utils])
  (:import (java.io IOException)
           (java.nio BufferOverflowException)))

;; TODO schedule filter
;; TODO parse filter
;; TODO hash digests
;; TODO binary parser
;; TODO filter urls directly via robots when coming from the same page
;; TODO send to other agents
;; TODO check for location header

;; Currently the parsing-thread is only adapted for html string parsing.
;; This should become a stream parser. Maybe also enable binary parsing etc...

;; TODO maybe extract the logic to when actually enqueue the url to the sieve
;; into a new module. See LinkReceiver in BUBing.
;; TODO once we do digest computation the parsing should also be done only once the
;; parsing logic probably be moved into a seperate module
(defn parse-fetched-data [{:keys [fetched-data store sieve url-cache
                                  scheme+authority-to-count runtime-config] :as _parsing-thread-data}]
  (let [{:keys [response url]} fetched-data
        ;; TODO add parsers here
        ;; TODO don't parse if not html
        urls (try
               (->> response :body (extraction/html->links :jericho)
                    (map #(uri/join url (uri/uri %))))
               (catch BufferOverflowException _e
                 (log/warn :buffer-overflow {:url url}))
               (catch IOException e
                 (log/warn :io-exception {:url url
                                          :exception-type (type e)})))]
    (when response
      (store/store store url response))
    (doseq [url urls]
      (when-not (lru/check url-cache url)
        (lru/add url-cache url)
        (let [scheme+authority (url/scheme+authority url)]
          (when (< (get @scheme+authority-to-count scheme+authority 0)
                   (:ramper/max-urls-per-scheme+authority @runtime-config))
            (sieve/enqueue sieve (str url))))))))

(defn parsing-thread [{:keys [_store _sieve _url-cache _scheme+authority-to-count results-queue] :as thread-data}
                      index stop-chan]
  (thread-utils/set-thread-name (str *ns* "-" index))
  (thread-utils/set-thread-priority Thread/MIN_PRIORITY)
  (try
    (loop [i 0]
      (when-not (async/poll! stop-chan)
        (if-let [fetched-data (pq/dequeue! results-queue)]
          ;; TODO should this be stopped in case of some error?
          (do (parse-fetched-data (assoc thread-data :fetched-data fetched-data))
              (recur 0))
          (let [time (bit-shift-left 1 (max 10 i))]
            (Thread/sleep time)
            (log/info :fetching-thread {:sleep-time time})
            (recur (inc i))))))
    (catch Throwable t
      (log/error :unexpected-ex {:ex t})))
  (log/info :graceful-shutdown {:type :parsing-thread
                                :index index})
  true)

(comment
  (uri/join (uri/uri "https://finnvolkel.com/a/foo?query=1") (uri/uri "/b/c?query=4#foo"))
  (uri/join (uri/uri "https://finnvolkel.com/a/foo?query=1") (uri/uri "https://yoolo.com/b/c?query=4#foo"))
  )
