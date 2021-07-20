(ns ramper.store
  "A set of protocols for a Store."
  (:refer-clojure :exclude [read]))

(defprotocol Store
  (store
    [this url repsonse]
    [this url repsonse is-duplicate content-digest guessed-charset]))

(defprotocol StoreReader
  (read [this]))
