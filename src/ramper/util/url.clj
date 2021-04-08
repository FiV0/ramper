(ns ramper.util.url
  (:require [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [lambdaisland.uri.normalize :as normalize]))

(defn base [uri-like]
  (let [old (uri/uri uri-like)]
    (assoc (uri/uri "")
           :scheme (:scheme old)
           :host (:host old))))

(defn domains [uri-like]
  (let [uri (uri/uri uri-like)]
    (str/split (:host uri) #".")))

(defn remove-www [uri-like]
  (let [uri (uri/uri uri-like)]
    (cond-> uri
      (str/starts-with? (:host uri) "www.")
      (assoc :host (subs (:host uri) 4)))))

(defn valid? [uri-like]
  (let [uri (uri/uri uri-like)]
    (and (:scheme uri) (:host uri) true)))

(defn normalize [uri-like]
  (let [uri (-> (uri/uri uri-like) normalize/normalize)]
    (dissoc uri :fragment)))

(defn to-byte-array [uri]
  (->> (str uri)
       (map byte)
       byte-array
       bytes))

(defn from-byte-array [bytes]
  (uri/uri (String. bytes)))


(comment
  (remove-www "https://harbour.space/")
  (remove-www "https://www.harbour.space/foo/bar")


  (valid? "mailto:foo@example.com")
  (valid? "https://harbour.space/\\a")

  (slurp "https://harbour.space/\ra")

  (from-byte-array (to-byte-array (uri/uri "https://harbour.space/")))

  (into {} (uri/uri ))


  )
