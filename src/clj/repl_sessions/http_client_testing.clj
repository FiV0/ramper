(ns repl-sessions.http-client-testing
  (:require [clj-http.client]
            [clj-http.conn-mgr :as conn]
            [clj-http.core :as core]
            [hato.client]
            [org.httpkit.client]
            [repl-sessions.utils :refer [wrap-timer]])
  (:import (org.xbill.DNS Address)))

;; test 3 things
;; - simple fetching with get
;; - simple fetching with keepalive
;; - simple fetching with keepalive + ip-address

(def cm (conn/make-reusable-conn-manager {}))
(def clj-http-client (core/build-http-client {} false cm))

(-> (clj-http.client/get "https://httpbin.org/get" {:connection-manager cm :http-client clj-http-client})
    :request-time)
;; second eval
;; => 110
;; first eval
;; => 448

(-> (hato.client/get "https://httpbin.org/get") :request-time)
;; second eval
;; => 343
;; first eval
;; => 572

(wrap-timer
 (-> (org.httpkit.client/get "https://httpbin.org/get") deref ))
;; second eval
;; => 113
;; first eval
;; => 478
