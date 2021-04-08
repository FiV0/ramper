(ns ramper.util.extraction.jsoup
  (:require [clojure.string :as str]
            [ramper.util.extraction :as extraction])
  (:import (org.jsoup Jsoup)
           (org.jsoup.nodes Element)
           (org.jsoup.select Elements)))

(defn parse [^String html]
  (when html
    (Jsoup/parse html)))

(defmethod extraction/html->text :jsoup [_ html-str]
  (when-let [doc (parse html-str)]
    (str
     (some-> doc .head .text) " "
     (some-> doc .body .text))))



(comment
  (def html-str (slurp "http://finnvolkel.com"))

  (extraction/html->text :jsoup html-str))

(defmethod extraction/html->title :jsoup [_ html-str]
  (some-> (parse html-str) (.select "title") .first .text))

(defmethod extraction/html->links :jsoup [_ html-str]
  (->> (.select (parse html-str) "a[href]")
       (remove #(some-> % (.attr "rel") (.contains "nofollow")) )
       (map #(.attr % "href"))))

(comment
  (def reddit-str (slurp "https://www.reddit.com/"))
  (def hckn (slurp "https://news.ycombinator.com/"))

  (extraction/html->title :jsoup hckn)

  (extraction/html->links :jsoup html-str)
  (extraction/html->links :jsoup reddit-str)

  ((set (extraction/html->links :jsoup reddit-str))
   "https://www.reddit.com/r/AskReddit/comments/kpgl66/serious_redditors_who_gave_up_pursuing_their/" )
  )

(defmethod extraction/html->lang-attrs :jsoup [_ html-str]
  (let [elements (-> (parse html-str) (.select "[lang]"))]
    (->> (map #(.attr % "lang") elements)
         (remove nil?))))

(comment
  (extraction/html->lang-attrs :jsoup html-str))

(defmethod extraction/html->code :jsoup [_ html-str]
  (let [elements (-> (parse html-str) (.select "code"))]
    (map #(hash-map :code (.text %)
                    :lang-info (->> (.attributes %)
                                    .asList
                                    (map (fn [attr] [(.getKey attr) (.getValue attr)]))))
         elements)))

(comment
  (def html-with-code (slurp "https://clojureverse.org/t/best-library-for-querying-html/1103"))

  (extraction/html->code :jsoup html-with-code))


(defn extract-script [script-tag]
  [(.attr script-tag "src") (.html script-tag)])

(defn mathjax? [parsed-html]
  (let [script-tags (.select parsed-html "script")]
    (->> (map extract-script script-tags)
         (filter #(or (str/includes? (first %) "MathJax")
                      (str/includes? (second %) "MathJax")))
         seq)))

(defmethod extraction/html->math :jsoup [_ html-str]
  (let [parsed (parse html-str)]
    (if-let [elements (-> parsed (.select  "math") seq)]
      (map #(.text %) elements)
      (if (mathjax? parsed)
        (extraction/find-latex (extraction/html->text :jsoup html-str))
        nil))))


(comment
  (def html-with-math
    (slurp "https://mathoverflow.net/questions/380081/if-exp-mu-n-n-in-mathbb-n-is-weakly-convergent-is-the-normalized-seque"))

  (def html-with-math2
    (slurp "https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation"))

  (def html-without-math (slurp "http://buffettfaq.com/"))

  (def html-with-math3
    (slurp "https://towardsdatascience.com/light-on-math-machine-learning-intuitive-guide-to-latent-dirichlet-allocation-437c81220158"))
  (def html-with-math4 (slurp "https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html"))

  (extraction/html->math :jsoup html-with-math)
  (extraction/html->math :jsoup html-with-math2)
  (extraction/html->math :jsoup html-without-math)
  (extraction/html->math :jsoup html-with-math3)
  (extraction/html->math :jsoup html-with-math4)

  (mathjax? (parse html-with-math))
  (mathjax? (parse html-without-math))

  (->> (-> (parse html-with-math) (.select "script"))
       ;; (drop 3) first
       (map extract-script)
       ;; (.html)
       )




  )
