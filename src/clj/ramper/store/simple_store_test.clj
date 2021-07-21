(ns ramper.store.simple-store-test
  (:refer-clojure :exclude [read])
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [ramper.store :refer [store read]]
            [ramper.store.simple-record :as rec]
            [ramper.store.simple-store :as store]
            [ramper.util :as util]
            [ramper.util.url-factory :as url-factory])
  (:import (java.io PushbackReader)))

(defn- slurp-edn-resource [file-name]
  (with-open [r (PushbackReader. (io/reader (io/file "resources/test" file-name)))]
    (edn/read r)))

(deftest simple-store-simple-test
  (testing "simple-store without concurrency"
    (let [urls (repeatedly 3 #(url-factory/rand-url))
          responses (for [i (range 3)]
                      (slurp-edn-resource (str "site" i ".edn")))
          store-dir (util/temp-dir "simple-store-test")
          s (store/simple-store store-dir)
          sr (store/simple-store-reader store-dir)]
      (doseq [[url resp] (map vector urls responses)]
        (store s url resp))
      (.close s)
      (doseq [[url resp] (map vector urls responses)]
        (is (= (rec/simple-record url resp) (read sr))))
      (is (nil? (read sr)))
      (.close sr))))

(deftest simple-store-multithreaded-test
  (testing "simple-store multi-threaded"
    (let [urls (repeatedly 3 #(url-factory/rand-url))
          responses (for [i (range 3)]
                      (slurp-edn-resource (str "site" i ".edn")))
          store-dir (util/temp-dir "simple-store-test")
          s (store/simple-store store-dir)
          sr (store/simple-store-reader store-dir)
          threads (for [[url resp] (map vector urls responses)]
                    (future
                      (store s url resp)))]
      (run! deref threads)
      (.close s)
      (loop [i 0]
        (if (< i 3)
          (do
            (is (not (nil? (read sr))))
            (recur (inc i)))
          (is (nil? (read sr)))))
      (.close sr))))

(comment
  ;; generating some data for testing
  (require '[clj-http.client :as client])
  (defn remove-uncessary [resp]
    (select-keys resp [:headers :status :body]))

  (def site0 (-> "https://news.ycombinator.com" client/get remove-uncessary))
  (def site1 (-> "https://finnvolkel.com" client/get remove-uncessary))
  (def site2 (-> "https://clojure.org" client/get remove-uncessary))

  (defn write-to-resources [file-name data]
    (with-open [w (io/writer (io/file "resources/test" file-name))]
      (binding [*out* w]
        (pr data))))

  (write-to-resources "site0.edn" site0)
  (write-to-resources "site1.edn" site1)
  (write-to-resources "site2.edn" site2)

  (slurp-edn-resource "site1.edn"))
