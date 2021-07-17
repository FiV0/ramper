(ns ramper.util.robots
  "Functions for dealing "
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [ramper.util :as util]
            [ramper.util.url :as url-util]))

(def ramper-agent "ramper-agent")

(defn robots-txt
  "Returns the \"robots.txt\" path of an `url-like` object."
  [uri-like]
  (assoc (url-util/base uri-like)
         :path "/robots.txt"))

(defn parse-robots-txt
  "Parses a robot txt as string.

  Returns a map of
  -`:disallow`- the disallowed paths, sorted
  -`:crawl-delay`- the crawl delay if present
  -`:sitemap` - the sitemap url if present"
  ([txt] (parse-robots-txt txt ramper-agent))
  ([txt agent]
   (loop [[line & lines] (str/split txt #"\n")
          res {:disallow [] :sitemap [] :crawl-delay nil}
          relevant false]
     (if line
       (let [[type value] (->> (str/split line #" ") (remove empty?))]
         (cond
           ;; empty line, new block starts
           (empty? line)
           (recur lines res false)

           ;; check whether the next block concerns us
           (and (util/compare-ignore-case "user-agent:" type)
                (or (= "*" value) (= agent value)))
           (recur lines res true)

           ;; append disallows if relevant for us
           (and (util/compare-ignore-case "disallow:" type)
                relevant)
           (recur lines (update res :disallow conj value) relevant)

           ;; parse crawl delay if relevant for us
           (and (util/compare-ignore-case "crawl-delay:" type)
                relevant)
           (recur lines (assoc res :crawl-delay (Integer/parseInt value)) relevant)

           ;; add sitemap if present
           (util/compare-ignore-case "sitemap:" type)
           (recur lines (update res :sitemap conj value) relevant)

           :else ;; something else
           (recur lines res relevant)))
       (update res :disallow sort)))))

;; TODO optimisation with streams from http response directly
;; TODO sort prefix free disallow

(comment
  (def txt (slurp (str (robots-txt "https://news.ycombinator.com/foo/"))))
  (def clojure-robots (slurp (str (robots-txt "https://clojure.org"))))
  (def youtube-robots (slurp (str (robots-txt "https://youtube.com"))))

  (parse-robots-txt txt)
  (parse-robots-txt clojure-robots)
  (parse-robots-txt youtube-robots)

  )
