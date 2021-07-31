(ns ramper.util.thread
  "Utility functions for working with Java threads."
  (:require [clojure.core.async :as async]))

(defn set-thread-name
  "Set the name of the current thread to `name`."
  ([name] (set-thread-name name (Thread/currentThread)))
  ([name thread] (.setName thread name)))

(defn set-thread-priority
  "Sets the `priority` of the current thread."
  [priority] (.setPriority (Thread/currentThread) priority))

(defn get-threads
  "Returns a list of threads with the given `name`."
  [name]
  (->> (Thread/getAllStackTraces) .keySet (filter #(= name (.getName %)))))

(defrecord ThreadWrapper [thread stop-chan])

(defn thread-wrapper
  "Creates a new thread with the Runnable `thread-fn.`

  `thread-fn` should be a function of one argument that takes a channel of one argument
  and repeatedly calls `clojure.core.async/poll!` on the channel and gracefully stops
  when a result is returned from the channel."
  [thread-fn]
  (let [stop-chan (async/chan)
        thread (async/thread (apply thread-fn [stop-chan]))]
    (ThreadWrapper. thread stop-chan)))

(defn stop
  "Signals the thread lauched with thread-wrapper to gracefully shut down."
  [{:keys [stop-chan thread] :as thread-wrapper}]
  {:pre [(instance? ThreadWrapper thread-wrapper)]}
  (async/>!! stop-chan true)
  (async/<!! thread))
