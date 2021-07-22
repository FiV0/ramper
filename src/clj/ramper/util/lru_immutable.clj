(ns ramper.util.lru-immutable
  "A thin wrapper around `clojure.core.cached`"
  (:require [clojure.core.cache.wrapped :as cw]
            [ramper.util.lru :as lru :refer [Cache add check]]))

(deftype LruCacheImmutable [cache hash-fn]
  Cache
  (add [this item]
    (cw/through-cache cache (hash-fn item) (constantly true)))
  (check [this item]
    (cw/lookup (.cache this) (hash-fn item))))

(defn create-lru-cache
  ([threshold hash-fn] (create-lru-cache {} threshold hash-fn))
  ([data threshold hash-fn]
   (->LruCacheImmutable (cw/lru-cache-factory data :threshold threshold) hash-fn)))

(comment
  (require '[ramper.util :as util])
  (def cache (create-lru-cache {} 2 util/hash-str))
  (add cache "foo bar")
  (check cache "foo bar")
  (check cache "dafafa")
  (add cache "dafafa")
  (add cache "dafafa1"))
