(ns ramper.startup-configuration
  "Read and check a startup-configuration."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.util :as util]))

(defn write-urls
  "Writes plain string `urls` to a `seed-file`."
  [seed-file urls]
  (with-open [wrt (io/writer seed-file)]
    (doseq [url urls]
      (.write wrt url)
      (.write wrt "\n"))))

(defn write-urls*
  "Writes lambdaisland.uri.URI `urls` to a `seed-file`."
  [seed-file urls]
  (write-urls seed-file (map str urls)))

;; See also repl-sessions.url-extraction
(comment
  (require '[repl-sessions.url-extraction :as urls])
  (write-urls (io/file "resources/seed.txt") (urls/get-urls))
  (write-urls* (io/file "resources/seed.txt") (map uri/uri (urls/get-urls))))

(defn read-urls
  "Reads urls as plain strings from a `seed-file`."
  [seed-file]
  (with-open [rdr (io/reader seed-file)]
    (doall (line-seq rdr))))

(defn read-urls*
  "Read urls as lambdaisland.uri.URI's from a `seed-file`."
  [seed-file]
  (->> (read-urls seed-file)
       (map uri/uri)))

(comment
  (read-urls (io/resource "seed.txt") )
  (read-urls* (io/resource "seed.txt") )
  )

(s/def ::startup-config (s/keys :req [:ramper/aux-buffer-size
                                      :ramper/cookies-max-byte-size
                                      :ramper/dns-threads
                                      :ramper/fetching-threads
                                      :ramper/init-front-size
                                      :ramper/ip-delay
                                      :ramper/is-new
                                      :ramper/keepalive-time
                                      ;; :ramper/max-urls
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

(defn- read-config* [file]
  {:post [(s/valid? ::startup-config %)]}
  (merge
   (edn/read-string (slurp (io/file (io/resource "default-config.edn"))))
   (edn/read-string (slurp (io/file file)))))

(defn read-config
  "Reads a ramper config from `file`, adds missing keys from a default config."
  [file]
  (let [{:ramper/keys [root-dir seed-file is-new] :as config}
        (-> (read-config* file)
            (update :ramper/root-dir #(-> % io/file util/make-absolute))
            (update :ramper/seed-file #(-> % io/file util/make-absolute)))]
    ;; TODO add checks for restarts
    (when-not (.exists root-dir)
      (log/warn :missing-root-dir {:dir root-dir})
      (.mkdirs root-dir))
    (when (and is-new (not (util/empty-dir? root-dir)))
      (throw (IllegalStateException. (str ":ramper/root-dir " root-dir " must be empty!"))))
    (when-not (.isFile seed-file)
      (throw (IllegalArgumentException. (str ":ramper/seed-file" seed-file " non existant!"))))
    config))

(comment
  (read-config* (io/resource "example-config.edn"))
  (read-config (io/resource "example-config.edn"))

  )
