(ns ramper.util.url
  "Helper functions for working with lambdaisland.uri.URI."
  (:refer-clojure :exclude [uri?])
  (:require [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [lambdaisland.uri.normalize :as normalize]
            [ramper.util :as util]
            [ramper.util.byte-serializer :as byte-serializer :refer [ByteSerializer]]            )
  (:import (lambdaisland.uri URI)))

(defn base
  "Returns only the scheme + authority of an uri-like object as
  an URI."
  [uri-like]
  (let [{:keys [scheme host]} (uri/uri uri-like)]
    (assoc (uri/uri "") :scheme scheme :host host)))

(def scheme+authority base)

(defn path+queries
  "Returns only the path + queries of an uri-like object as an URI."
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

(defn uri?
  "Returns true if `uri-like` is a lambdaisland.uri.URI."
  [uri-like]
  (instance? URI uri-like))

(defn hash-url
  "Hashes a lambdaisland.uri.URI with the 128 bit MurmurHash3
  yielding a vector of two longs."
  [url]
  {:pre [(instance? URI url)]}
  (util/hash-str (str url)))

(deftype UrlByteSerializer []
  ByteSerializer
  (to-stream [_ os x] (->> x str util/string->bytes (byte-serializer/write-array os)))
  (from-stream [_ is] (-> (byte-serializer/read-array is) util/bytes->string uri/uri))
  (skip [_ is] (byte-serializer/skip-array is)))

(defn url-byte-serializer
  "Returns a serializer implementing `ramper.util.byte-serializer.ByteSerializer`
  for `lambdaisland.uri.URI`."
  []
  (->UrlByteSerializer))

(comment
  (remove-www "https://harbour.space/")
  (remove-www "https://www.harbour.space/foo/bar")

  (valid? "mailto:foo@example.com")
  (valid? "https://harbour.space/")

  (uri? "https://harbour.space/")
  (uri? (uri/uri "https://harbour.space/"))

  (from-byte-array (to-byte-array (uri/uri "https://harbour.space/")))

  (into {} (uri/uri "https://hello.world/a/path?query=1&second=2#fragment"))

  (-> (uri/uri "https://hello.world/a/path?query=1&second=2#fragment") path+queries str)

  (-> (uri/uri "https://harbour.space/") hash-url))
