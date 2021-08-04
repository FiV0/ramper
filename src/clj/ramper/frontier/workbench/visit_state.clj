(ns ramper.frontier.workbench.visit-state
  (:require [ramper.util.byte-serializer :as byte-serializer]
            [taoensso.nippy :as nippy]))

(def robots-path "/robots.txt")

;; visit-state documentation
;;
;; scheme+authority - is the scheme and authority as a string visited by this state
;; next-fetch - the minimum time when the next fetch for this state is possible.
;;              This next-fetch only concerns this scheme and authority pair and
;;              might be different to the next-fetch of the workbench entry. See
;;              also ramper.frontier.workbench.workbench-entry.
;; robots-filter - the robots forbidden prefixes
;; cookies - cookies associated with this visit state
;; last-exception - the last exception that occured for this visit state (if any)
;; retries - the number of retries with respect to the above exception
;; path-queries - the path queries associated with this visit state
;;
;; FIXME: this is ugly
;; visit states might also optionally contain some info of the workbench-entry
;;
;; ip-address - the ip-address of the scheme+authority, get assoced when leaving
;;              an workbench entry.
;; locked-entry - the visit-state is currently not in the workbench and indicates that
;;                a workbench entry is currently locked because of it.

;; TODO robots stuff
;; TODO think about whether having explicit getters/setters or just assoc data directly.
;; tending to the latter.

(defrecord VisitState [scheme+authority next-fetch robots-filter cookies
                       last-exception retries path-queries]
  Object
  (toString [_this] (str "[" scheme+authority "(" (count path-queries) ")]")))

(defn visit-state
  ([scheme+authority] (visit-state scheme+authority []))
  ([scheme+authority path-queries]
   (->VisitState scheme+authority 0 [] [] nil 0 (into clojure.lang.PersistentQueue/EMPTY path-queries))))

(defn enqueue-robots
  "Enqueues a robots.txt path to path queries of the `visit-state`."
  [^VisitState visit-state]
  (update visit-state :path-queries conj robots-path))

(defn remove-robots
  "Removes all robots.txt paths from the path-queries."
  [^VisitState {:keys [path-queries] :as visit-state}]
  (->> path-queries
       (remove #(= robots-path %))
       (into clojure.lang.PersistentQueue/EMPTY)
       (assoc visit-state :path-queries)))

(defn enqueue-path-query
  "Enqueue a `path-query` to the given `visit-state`"
  [^VisitState visit-state path-query]
  (update visit-state :path-queries conj path-query))

(defn first-path
  "Peeks at the first path query."
  [^VisitState visit-state]
  (-> visit-state :path-queries peek))

(defn dequeue-path-query
  "Dequeues a path-query from the given `visit-state`"
  [^VisitState visit-state]
  (update visit-state :path-queries pop))

(defn size
  "Size (in urls) of the `visit-state`."
  [^VisitState visit-state]
  (-> visit-state :path-queries count))

(defn clear
  "Clears the `visit-state` of all the urls it contains."
  [^VisitState visit-state]
  (assoc visit-state :path-queries clojure.lang.PersistentQueue/EMPTY))

(defn fetchable?
  "Returns true when the `visit-state` is fetchable."
  [^VisitState {:keys [next-fetch] :as visit-state} time]
  (and (> (size visit-state) 0)
       (>= next-fetch time)))

(defn add-cookies
  "Adds cookies to the `visit-state`."
  [^VisitState visit-state cookies]
  (update visit-state :cookies into cookies))

(defn set-cookies
  "Sets the cookies of the `visit-state`."
  [^VisitState visit-state cookies]
  (assoc visit-state :cookies cookies))

(nippy/extend-freeze VisitState :visit-state/serialize
  [this os]
  (.writeUTF os (:scheme+authority this))
  (byte-serializer/write-array os (-> this :path-queries seq nippy/freeze)))


(nippy/extend-thaw :visit-state/serialize
  [is]
  (visit-state (.readUTF is) (-> is byte-serializer/read-array nippy/thaw)))

;; version not using byte-serializer
;; seems to consume a little more memory
(comment
  (nippy/extend-freeze VisitState :visit-state/serialize
    [this os]
    (.writeUTF os (:scheme+authority this))
    (let [frozen-path-queries (-> this :path-queries seq nippy/freeze)]
      (.writeInt os (count frozen-path-queries))
      (.write os frozen-path-queries)))


  (nippy/extend-thaw :visit-state/serialize
    [is]
    (let [scheme+authority (.readUTF is)
          len (.readInt is)
          ba (byte-array len)]
      (.readFully is ba)
      (visit-state scheme+authority (nippy/thaw ba)))))

(comment
  (def vs (-> (visit-state "https://finnvolkel.com")
              (enqueue-path-query "/a/foo")
              (enqueue-path-query "/a/bar")))

  (-> vs nippy/freeze count)
  (-> vs nippy/freeze nippy/thaw))
