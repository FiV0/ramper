(ns ramper.store.parallel-buffered-store-test
  (:refer-clojure :exclude [read])
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [ramper.store :refer [store read]]
            [ramper.store.simple-store :as store]
            [ramper.store.parallel-buffered-store :as pstore]
            [ramper.util :as util]
            [ramper.util.url-factory :as url-factory])
  (:import (java.io PushbackReader)))

(defn- slurp-edn-resource [file-name]
  (with-open [r (PushbackReader. (io/reader (io/file "resources/test" file-name)))]
    (edn/read r)))

(deftest parallel-buffered-store-multithreaded-test
  (testing "parallel-buffered-store-test multi-threaded"
    (let [nb-threads 50
          items-per-thread 100
          urls (repeatedly 3 #(url-factory/rand-url))
          responses (for [i (range 3)]
                      (slurp-edn-resource (str "site" i ".edn")))
          store-dir (util/temp-dir "parallel-buffered-store-test")
          s (pstore/parallel-buffered-store store-dir)
          sr (store/simple-store-reader store-dir)
          threads (repeatedly nb-threads
                              #(future
                                 (doseq [[url resp] (->> (map vector urls responses)
                                                         cycle
                                                         (take items-per-thread))]
                                   (store s url resp))))]
      (run! deref threads)
      (.close s)
      ;; parallel read
      #_(->> #(future
                (loop [i 0]
                  (when (< i items-per-thread)
                    (read sr)
                    (recur (inc i)))))
             (repeatedly nb-threads)
             (run! deref))
      (loop [i 0]
        (when (< i (* nb-threads items-per-thread))
          (is (not (nil? (read sr))))
          (recur (inc i))))
      (is (nil? (read sr)))
      (.close sr))))
