(ns ramper.util.extraction)

(defmulti html->text
  "Extracts text from html-str. Type corresponds to the
  underlying implementation."
  (fn [type _html-str] type))

(defmethod html->text :default [_ _]
  (throw (Exception. "Unknown html engine.")))

(defmulti html->title
  "Extracts the title from html-str. Type corresponds to the
  underlying implementation."
  (fn [type _html-str] type))

(defmethod html->title :default [_ _]
  (throw (Exception. "Unknown html engine.")))

(defmulti html->links
  "Extracts links from html-str. Respects no-follow.
  Type corresponds to the underlying implementation."
  (fn [type _html-str] type))

(defmethod html->links :default [_ _]
  (throw (Exception. "Unknown html engine.")))

(defmulti html->lang-attrs
  "Extracts language attributes from html-str. Type corresponds to the
  underlying implementation."
  (fn [type _html-str] type))

(defmethod html->lang-attrs :default [_ _]
  (throw (Exception. "Unknown html engine.")))

(defmulti html->code
  "Extracts code from html-str. Type corresponds to the
  underlying implementation."
  (fn [type _html-str] type))

(defmethod html->code :default [_ _]
  (throw (Exception. "Unknown html engine.")))

(defmulti html->math
  "Extracts math markup from html-str. Type corresponds to the
  underlying implementation."
  (fn [type _html-str] type))

(defmethod html->math :default [_ _]
  (throw (Exception. "Unknown html engine.")))

(def ^:private doller-latex #"(?<![\$\\])([\$]{1,2})[^\$\n]+(?<![\\])\1(?!\$)")
(def ^:private square-brackets #"(?<!\[\\)(\\\[.*\\\])")
(def ^:private paren-brackets #"(?<!\(\\)(\\\(.*\\\))")

(defn find-latex [str]
  (concat (->> str (re-seq doller-latex) (map first))
          (->> str (re-seq square-brackets) (map first))
          (->> str (re-seq paren-brackets) (map first))))
