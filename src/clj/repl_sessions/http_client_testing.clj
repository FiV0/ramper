(ns repl-sessions.http-client-testing
  (:require [clj-http.client :as apache]
            [clj-http.conn-mgr :as conn]
            [clj-http.core :as core]
            [hato.client :as hato]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [org.httpkit.client :as httpkit]
            [ramper.util.extraction :as extract]
            [ramper.util.extraction.jericho]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving]
            [repl-sessions.url-extraction]
            [repl-sessions.utils :refer [wrap-timer]])
  (:import (java.net InetSocketAddress URI)
           (org.apache.http.conn.ssl NoopHostnameVerifier TrustAllStrategy TrustSelfSignedStrategy)
           (org.apache.http.ssl SSLContextBuilder)
           (org.apache.http.impl DefaultConnectionReuseStrategy)
           (org.apache.http.impl.client HttpClientBuilder HttpClients)
           (org.xbill.DNS Address)))

;; test 3 things
;; - simple fetching with get
;; - simple fetching with keepalive
;; - simple fetching with keepalive + ip-address

(def cm (conn/make-reusable-conn-manager {}))
(def clj-http-client (core/build-http-client {} false cm))

(-> (apache/get "https://httpbin.org/get" {:connection-manager cm :http-client clj-http-client})
    :request-time)
;; second eval
;; => 110
;; first eval
;; => 448

(-> (hato/get "https://httpbin.org/get") :request-time)
;; second eval
;; => 343
;; first eval
;; => 572

(wrap-timer
 (-> (httpkit/get "https://httpbin.org/get") deref ))
;; second eval
;; => 113
;; first eval
;; => 478

;; hato client
(def hato-client (hato.client/build-http-client {:connect-timeout 500}))

(-> (hato/get "https://httpbin.org/get" {:http-client hato-client}) :request-time)
;; second eval
;; => 120
;; first eval
;; => 481

;; httpkit client
(defn address-finder [^URI uri]
  (let [port (.getPort uri)
        port (cond
               (not= -1 port) port
               (= "https" (.getScheme uri)) 443
               (= "http" (.getScheme uri)) 80
               :else (throw (IllegalStateException. (str "Unknown scheme: " (.getScheme uri)))))]
    (InetSocketAddress. (Address/getByName (.getHost uri)) port)))

(comment
  (type (address-finder (URI. "https://httpbin.org/get")))
  )

(def httpkit-client (org.httpkit.client/make-client {:address-finder address-finder}))

(wrap-timer
 @(org.httpkit.client/get "https://httpbin.org/get" {:client httpkit-client}))

(def urls (repl-sessions.url-extraction/get-urls))
(count urls)

;; simple clj-http testing
(def dns-resolver (dns-resolving/java-dns-resolver))
(def cm (conn/make-reusable-conn-manager {:dns-resolver dns-resolver}))
(def client-builder (.. (HttpClients/custom) (setConnectionReuseStrategy DefaultConnectionReuseStrategy/INSTANCE )))
(def clj-http-client (core/build-http-client {:http-client-builder client-builder} false cm))

(wrap-timer
 (doseq [url (take 100 urls)]
   (try
     (apache/get url {:http-client clj-http-client
                      :socket-timeout 500 :connection-timeout 500})
     (catch Throwable t (log/info :get-error
                                  {:cause (-> (Throwable->map t) :cause)
                                   :url url})))))
;; without custom dns resolver
;; => 62592
;; with custom dns-resolver
;; => 79280

;; simple hato testing
;; TODO no socket-timeout?
(wrap-timer
 (doseq [url (take 100 urls)]
   (try
     (hato/get url {:http-client hato-client})
     (catch Throwable t (log/info :get-error
                                  {:cause (-> (Throwable->map t) :cause)
                                   :url url})))))
;; => 68416

;; simple httpkit testing
(wrap-timer
 (doseq [url (take 100 urls)]
   (try
     @(httpkit/get url {:client httpkit-client
                        :timeout 500})
     (catch Throwable t (log/info :get-error
                                  {:cause (-> (Throwable->map t) :cause)
                                   :url url})))))
;; => 40293

(defn extract-relative [html parent-url]
  (->> (extract/html->links :jericho html)
       (filter uri/relative?)
       (map #(url/make-absolute parent-url %))))

;; keepalive testing
;; clj-http is still based on version 4.x of the apache client which does not support
;; http/2 and therefore also not keepalive
(wrap-timer
 (doseq [url (take 30 urls)]
   (try
     (let [{:keys [status body] :as _resp}
           (apache/get url {:http-client clj-http-client
                            :socket-timeout 500 :connection-timeout 500}
                       )]
       (if (= status 200)
         (doseq [url (extract-relative body (uri/uri url))]
           (try
             (apache/get (str url) {:http-client clj-http-client
                                    :socket-timeout 500 :connection-timeout 500})
             (catch Throwable _t nil)))
         (log/info :skipping {:url url})))
     (catch Throwable t (log/info :get-error
                                  {:cause (-> (Throwable->map t) :cause)
                                   :url url})))))

;; keepalive not properly supported in hato

;; http-kit supports it
(wrap-timer
 (doseq [url (take 30 urls)]
   (try
     (let [{:keys [status body] :as _resp}
           @(httpkit/get url
                         {:client httpkit-client
                          :timeout 500
                          :keepalive 30000})]
       (if (= status 200)
         (doseq [url (extract-relative body (uri/uri url))]
           @(httpkit/get (str url) {:client httpkit-client
                                    :timeout 500}))
         (log/info :skipping {:url url})))
     (catch Throwable t (log/info :get-error
                                  {:cause (-> (Throwable->map t) :cause)
                                   :url url})))))
;; => 128133

;; accept all certificates test
(defn set-hostname-verifier-noop [^HttpClientBuilder builder _req]
  (.setSSLContext builder (.. (SSLContextBuilder.) (loadTrustMaterial nil TrustAllStrategy/INSTANCE) build))
  (.setSSLHostnameVerifier builder NoopHostnameVerifier/INSTANCE))

(def clj-http-client (core/build-http-client {:http-builder-fns [set-hostname-verifier-noop]} false cm))

(def bad-ssl-urls '("https://expired.badssl.com"
                    "https://wrong.host.badssl.com"
                    "https://self-signed.badssl.com"
                    "https://untrusted-root.badssl.com"
                    "https://revoked.badssl.com"
                    "https://pinning-test.badssl.com"
                    "https://sha1-intermediate.badssl.com"))

(def bad-url "https://courses.cs.northwestern.edu/325/readings/graham/chap11-notes.html")

(apache/get
 "https://self-signed.badssl.com"
 {
  ;; :connection-manager cm
  :http-client clj-http-client
  ;; :http-builder-fns [set-hostname-verifier-noop]
  })
