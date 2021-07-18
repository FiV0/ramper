(ns ramper.util.url
  (:require [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [lambdaisland.uri.normalize :as normalize]))

(defn base
  "Returns only the scheme + authority of an uri-like object as
  an URI."
  [uri-like]
  (let [{:keys [scheme host]} (uri/uri uri-like)]
    (assoc (uri/uri "") :scheme scheme :host host)))

(def scheme+authority base)

(defn path+query
  "Returns only the path + quries of an uri-like object as an URI."
  [uri-like]
  (let [{:keys [path query]} (uri/uri uri-like)]
    (assoc (uri/uri "") :path path :query query)))

(defn domains
  "Returns the different domains of an uri-like object."
  [uri-like]
  (let [uri (uri/uri uri-like)]
    (str/split (:host uri) #".")))

(defn remove-www
  "Removes the www domain of an uri-like object."
  [uri-like]
  (let [uri (uri/uri uri-like)]
    (cond-> uri
      (str/starts-with? (:host uri) "www.")
      (assoc :host (subs (:host uri) 4)))))

(defn valid?
  "Tests whether `uri-like` is a valid url."
  [uri-like]
  (let [uri (uri/uri uri-like)]
    (and (:scheme uri) (:host uri) true)))

(defn normalize
  "Normalizes an `uri-like` object."
  [uri-like]
  (let [uri (-> (uri/uri uri-like) normalize/normalize)]
    (dissoc uri :fragment)))

(defn to-byte-array
  "Turns an `uri` into a byte-array."
  [uri]
  (->> (str uri)
       (map byte)
       byte-array))

(defn from-byte-array
  "Reads bytes as an URI."
  [bytes]
  (uri/uri (String. bytes)))

(comment
  (remove-www "https://harbour.space/")
  (remove-www "https://www.harbour.space/foo/bar")

  (valid? "mailto:foo@example.com")
  (valid? "https://harbour.space/")

  (from-byte-array (to-byte-array (uri/uri "https://harbour.space/")))

  (into {} (uri/uri "https://hello.world/a/path?query=1&second=2#fragment")))
