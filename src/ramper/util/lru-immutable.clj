(ns ramper.util.lru-immutable
  (:require [clojure.core.cache.wrapped :as cw]
            [ramper.util.lru :as lru :refer [Cache add check]])
  (:import (org.apache.commons.codec.digest MurmurHash3)))

(deftype LruCacheImmutable [cache hash-fn]
  Cache
  (add [this item]
    (cw/through-cache (.cache this) ((.hash-fn this) item) (constantly true)))
  (check [this item]
    (cw/lookup (.cache this) ((.hash-fn this) item))))

(defn create-lru-cache
  ([threshold hash-fn] (create-lru-cache {} threshold hash-fn))
  ([data threshold hash-fn]
   (->LruCacheImmutable (cw/lru-cache-factory data :threshold threshold)
                        (comp lru/bytes->murmur-hash hash-fn))))

(comment
  (def cache (create-lru-cache {} 2 lru/string->bytes))
  (add cache "foo bar")
  (check cache "foo bar")
  (check cache "dafafa")
  (add cache "dafafa")
  (add cache "dafafa1")

  )
