(ns ramper.util.url-factory
  "A test namespace for generating fake urls."
  (:require [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [ramper.util :as util]))

(defn rand-str-seq [len]
  (repeatedly len #(util/rand-str (rand-nth (range 3 10)))))

(defn rand-url []
  (-> (uri/map->URI
       {:scheme (rand-nth '("http" "https"))
        :host (str/join "." (rand-str-seq 3))
        :path (str "/" (str/join "/" (rand-str-seq (max 1 (rand-int 5)))))})
      (uri/assoc-query* (->>
                         (rand-str-seq 6)
                         (partition 2)
                         (map vec)
                         (into {})))))

(defn rand-url-seq [len]
  (repeatedly len #(rand-url)))

(defn assoc-random-path+queries [uri-like]
  (-> (uri/uri uri-like)
      (assoc :path (str "/" (str/join "/" (rand-str-seq (max 1 (rand-int 5))))))
      (uri/assoc-query* (->>
                         (rand-str-seq 6)
                         (partition 2)
                         (map vec)
                         (into {}))) ))

(defn rand-scheme+authority-seq [len]
  (let [base (uri/map->URI
              {:scheme (rand-nth '("http" "https"))
               :host (str/join "." (rand-str-seq 3))})]
    (repeatedly len #(assoc-random-path+queries base))))

(comment
  (rand-url-seq 5)
  (rand-scheme+authority-seq 5))
