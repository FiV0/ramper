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


(comment
  (require '[lambdacisland.uri :as uri])
  (s/check-asserts)

  (s/assert ::fetched-data {:url (uri/uri "https://finnvolkel.com")
                            :response {:status 200
                                       :body "foo"}})
  )
