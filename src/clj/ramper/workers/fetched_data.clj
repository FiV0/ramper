(ns ramper.workers.fetched-data
  (:require [clojure.spec.alpha :as s]
            [ramper.util.url :as url]))

(s/def ::response (s/keys :req-un [::headers
                                   ::status
                                   ::body]
                          :opt-un [::error]))

(s/def ::url url/uri?)

(s/def ::fetched-data (s/keys :req-un [::response
                                       ::url]))
