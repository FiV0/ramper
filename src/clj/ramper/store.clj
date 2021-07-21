(ns ramper.store
  "A set of protocols for a Store."
  (:refer-clojure :exclude [read]))

(defprotocol Store
  (store
    [this url repsonse]
    [this url repsonse is-duplicate content-digest guessed-charset]
    "Stores an url and its response in the store."))

(defprotocol StoreReader
  (read [this]
    "Reads a SimpleRecord from the store. Returns nil if no record is available."))
