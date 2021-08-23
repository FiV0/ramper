(ns ramper.startup-configuration
  "Read and check a startup-configuration."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]))

(defn write-urls [seed-file urls]
  (with-open [wrt (io/writer seed-file)]
    (doseq [url urls]
      (.write wrt url)
      (.write wrt "\n"))))

;; See also repl-sessions.url-extraction
(comment
  (require '[repl-sessions.url-extraction :as urls])
  (write-urls (io/file "resources/seed.txt") (urls/get-urls))
  )

(defn read-urls [seed-file]
  (with-open [rdr (io/reader seed-file)]
    (doall (line-seq rdr))))

(comment
  (read-urls (io/resource "seed.txt") )
  )

(s/def ::supported-keys (s/keys :req [:ramper/aux-buffer-size
                                      :ramper/cookies-max-byte-size
                                      :ramper/dns-threads
                                      :ramper/fetching-threads
                                      :ramper/init-front-size
                                      :ramper/ip-delay
                                      :ramper/is-new
                                      :ramper/keepalive-time
                                      :ramper/max-urls
                                      :ramper/max-urls-per-scheme+authority
                                      :ramper/parsing-threads
                                      :ramper/root-dir
                                      :ramper/scheme+authority-delay
                                      :ramper/seed-file
                                      :ramper/sieve-size
                                      :ramper/store-buffer-size
                                      :ramper/url-cache-max-byte-size
                                      :ramper/user-agent
                                      ;; :ramper/user-agent-from
                                      :ramper/workbench-max-byte-size]))

(defn read-config
  "Read the config from `file`, adds missing keys from a default config."
  [file]
  {:post [(s/assert ::supported-keys %)]}
  (merge
   (edn/read-string (slurp (io/file (io/resource "default-config.edn"))))
   (edn/read-string (slurp (io/file file)))))

(comment
  (read-config (io/resource "example-config.edn"))

  )
