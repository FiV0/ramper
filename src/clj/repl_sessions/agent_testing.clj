(ns repl-sessions.agent-testing
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [ramper.agent :as agent]
            [ramper.frontier :as frontier]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.stats :as stats]
            [ramper.util :as util]
            [ramper.util.thread :as thread-utils]))

(defn reinit-runtime-config
  "For testing purposes only."
  ([] (reinit-runtime-config runtime-config/runtime-config))
  ([runtime-config]
   (let [{:ramper/keys [root-dir]} @runtime-config
         root-dir (or root-dir (util/temp-dir "ramper-root"))
         frontier-dir (io/file root-dir "frontier")
         store-dir (io/file root-dir "store")]
     (when-not (.exists frontier-dir)
       (.mkdirs frontier-dir))
     (when-not (.exists store-dir)
       (.mkdirs store-dir))
     (swap! runtime-config assoc
            :ramper/root-dir root-dir
            :ramper/frontier-dir frontier-dir
            :ramper/store-dir store-dir
            :ramper/runtime-stop false))))

(def my-startup-config {:ramper/seed-file (io/resource "seed.txt")
                        :ramper/init-front-size 1000})

(def my-runtime-config (atom {:ramper/aux-buffer-size               (* 64 1024)
                              :ramper/cookies-max-byte-size         2048
                              :ramper/dns-threads                   1
                              :ramper/fetching-threads              1
                              :ramper/ip-delay                      2000 ;2 seconds
                              :ramper/is-new                        true
                              :ramper/keepalive-time                2000
                              :ramper/max-urls-per-scheme+authority 500
                              ;; Current estimation of the size of the front in ip addresses. Adaptively
                              ;; increased by the fetching threads whenever they have to wait to retrieve
                              ;; a visit state from the todo queue.
                              :ramper/parsing-threads               1
                              :ramper/required-front-size           1000
                              :ramper/runtime-pause                 false
                              :ramper/runtime-stop                  false
                              :ramper/scheme+authority-delay        2000 ;2 seconds
                              :ramper/sieve-size                    (* 64 1024)
                              :ramper/store-buffer-size             (* 64 1024)
                              :ramper/url-cache-max-byte-size       (* 1024 1024 1024)
                              :ramper/user-agent                    "ramper"
                              :ramper/workbench-max-byte-size       (* 512 1024 1024)
                              :ramper/root-dir (util/make-absolute "test-crawl")}))

(swap! my-runtime-config merge my-startup-config)

(swap! my-runtime-config assoc :ramper/runtime-stop true)
(swap! my-runtime-config assoc :ramper/runtime-stop false)

(s/valid? ::runtime-config/runtime-config @my-runtime-config)
(s/explain ::runtime-config/runtime-config @my-runtime-config)

(reinit-runtime-config my-runtime-config)

@stats/stats

(def my-agent (agent/agent* my-runtime-config))
(agent/stop my-agent)

(require '[ramper.frontier.workbench.visit-state :as visit-state]
         '[ramper.frontier.workbench.virtualizer :as virtual]
         '[ramper.util.url :as url])

(def my-virtual (-> my-agent :frontier :virtualizer))
(def url "https://github.com/jvm-profiling-tools/async-profiler")
(virtual/enqueue my-virtual (visit-state/visit-state (url/scheme+authority url))
                 url)


(thread-utils/get-threads "ramper.s")
(run! #(.stop %) (thread-utils/get-threads "ramper.s"))
