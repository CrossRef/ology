(ns ology.monet
    (:gen-class)
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.coerce :refer [to-date]])
    (:require [clojure.java.jdbc :as j])
    (:import com.mchange.v2.c3p0.ComboPooledDataSource)
    (:require [ology.config :refer [config]]))

; https://www.monetdb.org/sites/default/files/xqrymanual.pdf
; https://en.wikibooks.org/wiki/Clojure_Programming/Examples/JDBC_Examples
; http://clojure-doc.org/articles/ecosystem/java_jdbc/home.html

(Class/forName "nl.cwi.monetdb.jdbc.MonetDriver")
(def db {:classname "nl.cwi.monetdb.jdbc.MonetDriver"
               :subprotocol "monetdb"
               :user "monetdb"
               :debug true
               :password "monetdb"
               :subname "//localhost/ology"})

(defn pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               ;(.setProperties (doto (new java.util.Properties) (.setProperty "debug" "true")))
               (.setDriverClass (:classname spec)) 
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))

               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))] 
    {:datasource cpds}))

(def pooled-db (delay (pool db)))

(defn db-connection [] @pooled-db)

; Back-of-the-envelope metrics for 100,000 inserts.
; 100 - 19 sec
; 500 -  18 sec
; 1000 - 19 sec
; 2000 - 21 sec
; 10,000 - 47 sec
(def batch-size 500)

; Length of database fields are capped. 
; If these are exceeded (occasionally happens) just truncate.
(def max-doi-length 1024)
(def max-subdomain-length 128)
(def max-domain-length 128)
(def max-etld-length 128)

(defn trunc
  [s n]
  (subs s 0 (min (count s) n)))

(defn insert-log-entries
  "Insert multiple log entry in format [[date doi subdomain domain etld]]."
   [entries]
   (let [entries-safe (map (fn [[date doi subdomain domain etld]] [(to-date date)  (trunc doi max-doi-length) (trunc subdomain max-subdomain-length) (trunc domain max-domain-length) (trunc etld max-etld-length)]) entries)]
  (try
    (when (> (count entries) 0) (apply j/db-do-prepared (db-connection) "insert into resolutions (date, doi, subdomain, domain, etld) values (?, ?, ?, ?, ?)" entries-safe))
  (catch Exception ex (prn "Exception:" ex entries-safe)))))
      
      

