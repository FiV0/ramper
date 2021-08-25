(ns ramper.constants
  (:import (java.util.concurrent TimeUnit)))

;; Theses constants are copied from BUbing.

(def ^:private exception-to-wait-time
  {java.net.NoRouteToHostException                  (.. TimeUnit/HOURS (toMillis 1))
   java.net.SocketException                         (.. TimeUnit/MINUTES (toMillis 1))
   java.net.SocketTimeoutException                  (.. TimeUnit/MINUTES (toMillis 1))
   java.net.UnknownHostException                    (.. TimeUnit/HOURS (toMillis 1))
   javax.net.ssl.SSLPeerUnverifiedException         (.. TimeUnit/HOURS (toMillis 1))
   org.apache.http.client.CircularRedirectException 0
   org.apache.http.client.RedirectException         0
   org.apache.http.conn.ConnectTimeoutException     (.. TimeUnit/HOURS (toMillis 1))
   org.apache.http.ConnectionClosedException        (.. TimeUnit/MINUTES (toMillis 1))
   org.apache.http.conn.HttpHostConnectException    (.. TimeUnit/HOURS (toMillis 1))
   org.apache.http.NoHttpResponseException          (.. TimeUnit/MINUTES (toMillis 1))
   org.apache.http.TruncatedChunkException          (.. TimeUnit/MINUTES (toMillis 1))
   org.apache.http.MalformedChunkCodingException    (.. TimeUnit/MINUTES (toMillis 1))})

(defn get-exception-to-wait-time [ex]
  (get exception-to-wait-time ex (.. TimeUnit/HOURS (toMillis 1))))

(def ^:private exception-to-max-retries
  {java.net.UnknownHostException 2
   javax.net.ssl.SSLPeerUnverifiedException 0
   org.apache.http.client.CircularRedirectException 0
   org.apache.http.client.RedirectException 0
   org.apache.http.conn.ConnectTimeoutException 2
   org.apache.http.ConnectionClosedException 2
   org.apache.http.NoHttpResponseException 2
   org.apache.http.TruncatedChunkException 1
   org.apache.http.MalformedChunkCodingException 1})

(defn get-exception-to-max-retries [ex]
  (get exception-to-max-retries ex 5))

(def exception-host-killer
  #{java.net.NoRouteToHostException
    java.net.UnknownHostException
    java.net.SocketException
    javax.net.ssl.SSLPeerUnverifiedException
    org.apache.http.conn.ConnectTimeoutException})

(def death-interval
  "An interval after we consider a host dead."
  (.. TimeUnit/HOURS (toMillis 1)))

(def purge-delay
  "The delay since the last fetch when visit-states should be purged."
  (.. TimeUnit/DAYS (toMillis 1)))

(def min-flush-interval
  "The minimum time in milliseconds between two flushes."
  (.. TimeUnit/SECONDS (toMillis 10)))

(def stats-interval
  "The interval at which stats should be pushed to the stats thread."
  (.. TimeUnit/SECONDS (toMillis 10)))

(def shutdown-check-interval
  "The interval at which checks will be executed to check if the crawler
  should be shutdown due to configuations."
  (.. TimeUnit/SECONDS (toMillis 1)))
