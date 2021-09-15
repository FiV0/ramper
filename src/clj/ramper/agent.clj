(ns ramper.agent
  "This is the main entrypoint to the ramper crawler."
  (:refer-clojure :exclude [agent])
  (:require [clj-http.conn-mgr :as conn]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
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
;; TODO should the terminating conditions be checked in all threads or in one loop
;; that terminates the threads?
;; TODO cleanup confusing runtime-config as atom and as non atom approach
(defn start-todo-thread [runtime-config frontier]
  (async/thread (todo-thread/todo-thread (assoc frontier :runtime-config runtime-config))))

(defn start-done-thread [runtime-config frontier]
  (async/thread (done-thread/done-thread (assoc frontier :runtime-config runtime-config))))

(defn start-distributor-thread [runtime-config frontier]
  (async/thread (distributor/distributor-thread (assoc frontier :runtime-config runtime-config))))

(defn start-stats-loop [stats-atom runtime-config frontier stats-chan]
  (stats/stats-loop stats-atom runtime-config frontier stats-chan))

(defn start-dns-threads [runtime-config frontier]
  (let [{:ramper/keys [dns-threads]} @runtime-config]
    (for [i (range dns-threads)]
      (thread-utils/thread-wrapper (partial dns-resolving/dns-thread frontier i)))))

(defn start-fetching-threads [runtime-config frontier]
  (let [{:ramper/keys [fetching-threads]} @runtime-config]
    (for [i (range fetching-threads)]
      (thread-utils/thread-wrapper
       (partial fetching-thread/fetching-thread
                (assoc frontier :runtime-config runtime-config)
                i)))))

(defn start-parsing-threads [runtime-config frontier]
  (let [{:ramper/keys [parsing-threads]} @runtime-config]
    (for [i (range parsing-threads)]
      (thread-utils/thread-wrapper
       (partial parsing-thread/parsing-thread
                (assoc frontier :runtime-config runtime-config)
                i)))))

(defn init-thraeds [runtime-config frontier]
  (let [stats-chan (async/chan (async/sliding-buffer 100))
        dns-resolver (dns-resolving/global-java-dns-resolver)
        conn-mgr (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        frontier (assoc frontier
                        :dns-resolver dns-resolver
                        :connection-manager conn-mgr
                        :stats-chan stats-chan)]
    {:todo-thread (start-todo-thread runtime-config frontier)
     :done-thread (start-done-thread runtime-config frontier)
     :distributor (start-distributor-thread runtime-config frontier)
     :dns-threads-wrapped (doall (start-dns-threads runtime-config frontier))
     :fetching-threads-wrapped (doall (start-fetching-threads runtime-config frontier))
     :parsing-threads-wrapped (doall (start-parsing-threads runtime-config frontier))
     :stats-loop (start-stats-loop stats/stats runtime-config frontier stats-chan)}))

(defn cleanup-threads [{:keys [todo-thread done-thread distributor stats-loop
                               dns-threads-wrapped fetching-threads-wrapped
                               parsing-threads-wrapped]}]
  (doall (pmap #(thread-utils/stop %) dns-threads-wrapped))
  (doall (pmap #(thread-utils/stop %) fetching-threads-wrapped))
  (doall (pmap #(thread-utils/stop %) parsing-threads-wrapped))
  (if (async/<!! todo-thread)
    (log/info :proper-shutdown {:type :todo-thread})
    (log/warn :non-proper-shutdown {:type :todo-thread}))
  (if (async/<!! done-thread)
    (log/info :proper-shutdown {:type :done-thread})
    (log/warn :non-proper-shutdown {:type :done-thread}))
  (if (async/<!! distributor)
    (log/info :proper-shutdown {:type :distributor-thread})
    (log/warn :non-proper-shutdown {:type :distributor-thread}))
  (if (async/<!! stats-loop)
    (log/info :proper-shutdown {:type :stats-loop})
    (log/warn :non-proper-shutdown {:type :stats-loop}))
  (if (every? #(thread-utils/stopped? %) dns-threads-wrapped)
    (log/info :proper-shutdown {:type :dns-threads})
    (log/warn :non-proper-shutdown {:type :dns-threads}))
  (if (every? #(thread-utils/stopped? %) fetching-threads-wrapped)
    (log/info :proper-shutdown {:type :fetching-threads})
    (log/warn :non-proper-shutdown {:type :fetching-threads}))
  (if (every? #(thread-utils/stopped? %) parsing-threads-wrapped)
    (log/info :proper-shutdown {:type :parsing-threads})
    (log/warn :non-proper-shutdown {:type :parsing-threads})))

(declare stop)

(defn shutdown-checking-loop [{:keys [runtime-config frontier] :as agent}]
  (async/go-loop []
    (async/<! (async/timeout constants/shutdown-check-interval))
    (cond
      ;; stopped from externally
      (runtime-config/stop? @runtime-config)
      (log/info :graceful-shutdown {:type :shutdown-checking-loop})
      ;; reached a stopping condition
      (frontier/stop? runtime-config frontier)
      (do (log/info :shutdown-condition-reached {:urls-crawled @(:urls-crawled frontier)})
          (stop agent))
      :else (recur))))

;; TODO the term agent might be overloaded in Clojure
(defrecord Agent [runtime-config frontier threads])

;; TODO bring some better sturcture to this function, currently all over the place
(defn agent
  "Creates a ramper agent based on a startup config `file`."
  [file]
  (let [file (util/make-absolute file)]
    (when-not (.exists file)
      (throw (IllegalArgumentException. (str "config file: " file " does not exist!"))))
    (let [runtime-config (runtime-config/merge-startup-config
                          (startup-config/read-config file)
                          runtime-config/runtime-config)
          frontier (frontier/frontier @runtime-config)
          threads (init-thraeds runtime-config frontier)
          agent (->Agent runtime-config frontier threads)]
      (shutdown-checking-loop agent)
      agent)))

(defn agent*
  "Creates a ramper agent based on a `runtime-config` atom."
  [runtime-config]
  {:pre [(s/valid? ::runtime-config/runtime-config @runtime-config)]}
  (if (:ramper/runtime-stop @runtime-config)
    (log/warn :config-error {:ramper/runtime-stop true})
    (let [frontier (frontier/frontier @runtime-config)
          agent (->Agent runtime-config frontier (init-thraeds runtime-config frontier))]
      (shutdown-checking-loop agent)
      agent)))

(defn stop
  "Stops an agent and cleans up the lingering threads if any."
  [{:keys [runtime-config frontier threads] :as _agent}]
  (if (:ramper/runtime-stop @runtime-config)
    (log/warn :agent-already-stopped {})
    (do
      (swap! runtime-config assoc :ramper/runtime-stop true)
      ;; TODO move this cleanup stuff somewhere more consistent
      ;; (reset! stats/stats {})
      (cleanup-threads threads)
      (frontier/cleanup frontier))))
