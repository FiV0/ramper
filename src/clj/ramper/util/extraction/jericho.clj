(ns ramper.util.extraction.jericho
  (:require [clojure.java.io :as io]
            [ramper.util.extraction :as extraction])
  (:import (net.htmlparser.jericho HTMLElementName Source StartTagType TextExtractor)))

(defn source [html-str]
  (-> html-str
      .getBytes
      io/input-stream
      java.io.BufferedInputStream.
      Source.))

(defmethod extraction/html->text :jericho [_ html-str]
  (-> html-str
      source
      TextExtractor.
      .toString))

(comment
  (def html-str (slurp "http://finnvolkel.com"))

  (extraction/html->text :jericho html-str))

(defmethod extraction/html->title :jericho [_ html-str]
  (-> html-str source (.getAllElements HTMLElementName/TITLE) first .getTextExtractor .toString))

(comment
  (extraction/html->title :jericho html-str)
  )

(defmethod extraction/html->links :jericho [_ html-str]
  (let [tags (-> html-str source (.getAllElements HTMLElementName/A))]
    (->> (remove #(or (some-> % (.getAttributeValue "rel") (.contains "nofollow"))
                      (nil? (.getAttributeValue % "href"))) tags)
         (map #(.getAttributeValue % "href")))))

(comment
  (def reddit-str (slurp "https://www.reddit.com/"))
  (def vault-str (slurp "https://www.vaultproject.io"))

  (extraction/html->links :jericho html-str)
  (extraction/html->links :jericho reddit-str)
  (extraction/html->links :jericho vault-str)

  ((set (extraction/html->links :jericho reddit-str))
   "https://www.reddit.com/r/AskReddit/comments/kpgl66/serious_redditors_who_gave_up_pursuing_their/" )
  )


(defmethod extraction/html->lang-attrs :jericho [_ html-str]
  (let [tags (-> html-str source (.getAllTags StartTagType/NORMAL))]
    (->> (map #(.getAttributeValue % "lang") tags)
         (remove nil?))))

(comment
  (extraction/html->lang-attrs :jericho html-str))

(defn element->text [ele]
  (-> ele .getTextExtractor .toString))

(defn code-element->lang-info [ele]
  (->> ele
       .getAttributes
       (map #(vector (.getKey %) (.getValue %)))))

(defmethod extraction/html->code :jericho [_ html-str]
  (let [eles (-> html-str source (.getAllElements HTMLElementName/CODE))]
    (map #(hash-map :code (element->text %)
                    :lang-info (code-element->lang-info %)) eles)))

(comment
  (def html-with-code (slurp "https://clojureverse.org/t/best-library-for-querying-html/1103"))

  (extraction/html->code :jericho html-with-code))


(defmethod extraction/html->math :jericho [_ html-str]
  (if-let [eles (-> html-str source (.getAllElements "math") seq)]
    (map #(element->text %) eles)
    (extraction/find-latex (extraction/html->text :jericho html-str))))

(comment
  (def html-with-math
    (slurp "https://mathoverflow.net/questions/380081/if-exp-mu-n-n-in-mathbb-n-is-weakly-convergent-is-the-normalized-seque"))
  (def html-with-math2
    (slurp "https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation"))

  (extraction/html->math :jericho html-with-math)
  (extraction/html->math :jericho html-with-math2)
  )
