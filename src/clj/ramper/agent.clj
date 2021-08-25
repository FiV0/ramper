(ns ramper.agent
  "This is the main entrypoint to the ramper crawler."
  (:refer-clojure :exclude [agent])
  (:require [clj-http.conn-mgr :as conn]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [ramper.frontier :as frontier]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.startup-configuration :as startup-config]
            [ramper.stats :as stats]
            [ramper.util :as util]
            [ramper.util.thread :as thread-utils]
            [ramper.workers.distributor :as distributor]
            [ramper.workers.dns-resolving :as dns-resolving]
            [ramper.workers.done-thread :as done-thread]
            [ramper.workers.fetching-thread :as fetching-thread]
            [ramper.workers.parsing-thread :as parsing-thread]
            [ramper.workers.todo-thread :as todo-thread]))

;; TODO check runtime-config from time to time to adapt thread counts
;; TODO refactor the below
(defn start-todo-thread [runtime-config frontier]
  (async/thread (todo-thread/todo-thread (assoc frontier :runtime-config runtime-config))))

(defn start-done-thread [runtime-config frontier]
  (async/thread (done-thread/done-thread (assoc frontier :runtime-config runtime-config))))

(defn start-distributor-thread [runtime-config frontier stats-chan]
  (async/thread (distributor/distributor-thread (assoc frontier
                                                       :runtime-config runtime-config
                                                       :stats-chan stats-chan))))

(defn start-stats-loop [stats-atom runtime-config stats-chan]
  (stats/stats-loop stats-atom runtime-config stats-chan))

(defn start-dns-threads [runtime-config frontier]
  (let [{:ramper/keys [dns-threads]} @runtime-config]
    (for [i (range dns-threads)]
      (thread-utils/thread-wrapper (partial dns-resolving/dns-thread frontier i)))))

(defn start-fetching-threads [runtime-config frontier]
  (let [{:ramper/keys [fetching-threads]} @runtime-config]
    (for [i (range fetching-threads)]
      (thread-utils/thread-wrapper (partial fetching-thread/fetching-thread frontier i)))))

(defn start-parsing-threads [runtime-config frontier]
  (let [{:ramper/keys [parsing-threads]} @runtime-config]
    (for [i (range parsing-threads)]
      (thread-utils/thread-wrapper (partial parsing-thread/parsing-thread frontier i)))))

(defn init-thraeds [runtime-config frontier]
  (let [stats-chan (async/chan (async/sliding-buffer 5))
        dns-resolver (dns-resolving/global-java-dns-resolver)
        conn-mgr (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})]
    {:todo-thread (start-todo-thread runtime-config frontier)
     :done-thread (start-done-thread runtime-config frontier)
     :distributor (start-distributor-thread runtime-config frontier stats-chan)
     :stats-loop (start-stats-loop stats/stats runtime-config stats-chan)
     :dns-threads-wrapped (start-dns-threads runtime-config
                                             (assoc frontier :dns-resolver dns-resolver))
     :fetching-threads-wrapped (start-fetching-threads runtime-config
                                                       (assoc frontier :connection-manager conn-mgr))
     :parsing-threads-wrapped (start-parsing-threads runtime-config frontier)}))

(defn cleanup-threads [{:keys [todo-thread done-thread distributor stats-loop
                               dns-threads-wrapped fetching-threads-wrapped
                               parsing-threads-wrapped]}]
  (when-not (async/<!! todo-thread)
    (log/warn :non-proper-shutdown {:type :todo-thread}))
  (when-not (async/<!! done-thread)
    (log/warn :non-proper-shutdown {:type :done-thread}))
  (when-not (async/<!! distributor)
    (log/warn :non-proper-shutdown {:type :distributor-thread}))
  (when-not (async/<!! stats-loop)
    (log/warn :non-proper-shutdown {:type :distributor-thread}))
  (when-not (every? #(thread-utils/stop %) dns-threads-wrapped)
    (log/warn :non-proper-shutdown {:type :dns-threads}))
  (when-not (every? #(thread-utils/stop %) fetching-threads-wrapped)
    (log/warn :non-proper-shutdown {:type :fetching-threads}))
  (when-not (every? #(thread-utils/stop %) parsing-threads-wrapped)
    (log/warn :non-proper-shutdown {:type :parsing-threads})))

;; TODO is agent maybe already overloaded in Clojure
(defrecord Agent [runtime-config frontier threads])

(defn agent [file]
  (let [file (io/file file)]
    (when-not (.exists file)
      (throw (IllegalArgumentException. (str "config file: " file " does not exist!"))))
    (let [runtime-config (runtime-config/merge-startup-config
                          (startup-config/read-config (util/make-absolute file))
                          runtime-config/runtime-config)
          frontier (frontier/frontier @runtime-config)
          threads (init-thraeds runtime-config frontier)]
      (->Agent runtime-config frontier threads))))

(defn agent* []
  (let [frontier (frontier/frontier @runtime-config/runtime-config)]
    (->Agent runtime-config/runtime-config
             frontier
             (init-thraeds runtime-config/runtime-config frontier))))

(defn stop [{:keys [runtime-config threads] :as _agent}]
  (swap! runtime-config :ramper/runtime-stop true)
  (cleanup-threads threads))
