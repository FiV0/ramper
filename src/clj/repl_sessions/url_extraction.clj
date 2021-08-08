(ns repl-sessions.url-extraction
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; creating some seed urls from my bookmarks
(def bookmarks (-> (io/resource "bookmarks.json")
                   io/reader
                   (json/parse-stream true)))

(defn extract-uris [obj]
  (cond
    (:uri obj) (list (:uri obj))
    (map? obj) (->> obj
                    (filter (fn [[_ v]] (seqable? v)))
                    (mapcat (fn [[_ v]] (mapcat extract-uris v))))
    :else '()))

(def urls (->> (extract-uris bookmarks)
               (filter #(str/starts-with? % "http"))
               distinct
               sort))

(spit "resources/urls.txt" urls)

(defn get-urls []
  (let [f (-> (io/resource "urls.txt") io/file)]
    (if (.exists f)
      (read-string (slurp f))
      (throw (IllegalStateException. (str "File: " (.getPath f) " does not exist !!!"))))))
