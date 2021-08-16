(ns ramper.util.lru-immutable
  "A thin wrapper around `clojure.core.cached`"
  (:require [clojure.core.cache.wrapped :as cw]
            [ramper.util.lru :as lru :refer [Cache]]))

(deftype LruCacheImmutable [cache hash-fn]
  Cache
  (add [_this item]
    (cw/through-cache cache (hash-fn item) (constantly true)))
  (check [_this item]
    (cw/lookup cache (hash-fn item)))

  clojure.lang.Counted
  (count [_this]
    (count @cache)))

(defn create-lru-cache
  ([threshold hash-fn] (create-lru-cache {} threshold hash-fn))
  ([data threshold hash-fn]
   (->LruCacheImmutable
    (cw/lru-cache-factory (into {} (map #(vector (hash-fn %) true) data)) :threshold threshold)
    hash-fn)))

(comment
  (require '[ramper.util :as util])
  (require '[ramper.util.lru :refer [add check]])
  (def cache (create-lru-cache {} 2 util/hash-str))
  (add cache "foo bar")
  (check cache "foo bar")
  (check cache "dafafa")
  (add cache "dafafa")
  (add cache "dafafa1")
  (count cache))
