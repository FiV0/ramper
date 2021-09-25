(ns ramper.util.thread
  "Utility functions for working with Java threads."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]))

(defn set-thread-name
  "Set the name of the current thread to `name`."
  ([name] (set-thread-name name (Thread/currentThread)))
  ([name ^Thread thread] (.setName thread name)))

(defn set-thread-priority
  "Sets the `priority` of the current thread."
  [priority] (.setPriority (Thread/currentThread) priority))

(defn get-threads
  "Returns a list of threads that start with the given `name`."
  ([] (.keySet (Thread/getAllStackTraces)))
  ([name] (->> (Thread/getAllStackTraces)
               .keySet
               (filter #(str/starts-with? (.getName %) name)))))

(defrecord ThreadWrapper [thread stop-chan])

(defn thread-wrapper
  "Creates a new thread with the Runnable `thread-fn.`

  `thread-fn` should be a function of one argument that takes a channel of one argument
  and repeatedly calls `clojure.core.async/poll!` on the channel and gracefully stops
  when a result is returned from the channel."
  [thread-fn]
  ;; the buffer of 1 is important as poll might not be called in case of exceptions
  (let [stop-chan (async/chan 1)
        thread (async/thread (apply thread-fn [stop-chan]))]
    (ThreadWrapper. thread stop-chan)))

(defn stop
  "Signals the thread lauched with thread-wrapper to gracefully shut down."
  [{:keys [stop-chan] :as thread-wrapper}]
  {:pre [(instance? ThreadWrapper thread-wrapper)]}
  (async/put! stop-chan true))

(defn stopped?
  "Returns true when the thread shutdown gracefully."
  [{:keys [thread] :as thread-wrapper}]
  {:pre [(instance? ThreadWrapper thread-wrapper)]}
  (async/<!! thread))
