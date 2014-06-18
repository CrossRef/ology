(ns ology.monet
    (:gen-class)
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.coerce :refer [to-date]])
    (:require [clj-time.core :refer [date-time]])
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

(defn resolutions-table-size
  []
  (let [result (j/query (db-connection) "select count(*) as count from resolutions")]
    (-> result first :count)))

(defn aggregated-table-size
  []
  (let [result (j/query (db-connection) "select count(*) as count from resolutions_aggregate")]
    (-> result first :count)))


(defn produce-aggregations
  "For all the data in the resolutions table calculate aggregation and insert into resolution-aggregate table."
  []
  ; TODO this works for 1,000,000 entries but not 1,000,000,000. Need to partition.
  (j/db-do-commands (db-connection) "insert into resolutions_date_aggregate (count, subdomain, domain, etld, date, doi, y, m, d) select count(date) as count, subdomain, domain, etld, date, doi, extract(year from date), extract(month from date), extract(day from date) from resolutions group by date, subdomain, domain, etld, doi order by date"))

(defn flush-aggregations-table
  []
  (j/db-do-commands (db-connection) "delete from resolutions"))

; 'storage' interface.

(defn empty-string-to-nil [input] (if (= input "") nil input))

(defn query-days
  "For a query, return a vector of the count per day and the total for the whole period."
  [query group-by]
  {:pre [(#{"day" "month" "year"} group-by)]}
  (let [start-date (to-date (:start-date query))
        end-date (to-date (:end-date query))
        subdomain (empty-string-to-nil (:subdomain query))
        domain (empty-string-to-nil (:domain query))
        etld (empty-string-to-nil (:tld query))
        doi (empty-string-to-nil (:doi query))
        
        date-grouping (condp = group-by "day" "y, m, d" "month" "y, m" "year" "y")
        date-select (condp = group-by "day" "y, m, d" "month" "y, m" "year" "y")
        date-f (condp = group-by "day" (fn [row] {:date (date-time (:y row) (:m row) (:d row))}) "month" (fn [row] {:date (date-time (:y row) (:m row))}) "year" (fn [row] {:date (date-time (:y row))}))
        select-clause (str "select sum(count) as count, " date-select " from resolutions_date_aggregate")
        ; TODO Not very clever. Convert to KormaSQL.
        [where-clause where-args]
                      (if domain
                        (if subdomain
                          (if etld
                            (if doi
                              ["date >= ? and date <= ? and domain = ? and subdomain = ? and etld = ? and doi = ?" [start-date end-date domain subdomain etld doi]]
                              ["date >= ? and date <= ? and domain = ? and subdomain = ? and etld = ?" [start-date end-date domain subdomain etld]])
                            (if doi
                              ["date >= ? and date <= ? and domain = ? and subdomain = ? and doi = ?" [start-date end-date domain subdomain doi]]
                              ["date >= ? and date <= ? and domain = ? and subdomain = ?" [start-date end-date domain subdomain]]))
                          ; No subdomain
                          (if etld
                            (if doi
                              ["date >= ? and date <= ? and domain = ? and etld = ? and doi = ?" [start-date end-date domain etld doi]]
                              ["date >= ? and date <= ? and domain = ? and etld = ?" [start-date end-date domain etld]])
                            (if doi
                              ["date >= ? and date <= ? and domain = ? and doi = ?" [start-date end-date domain doi]]
                              ["date >= ? and date <= ? and domain = ?" [start-date end-date domain]])))                             
                        ; No domain.
                        (if subdomain
                          (if etld
                            (if doi
                              ["date >= ? and date <= ? and subdomain = ? and etld = ? and doi = ?" [start-date end-date subdomain etld doi]]
                              ["date >= ? and date <= ? and subdomain = ? and etld = ?"  [start-date end-date subdomain etld]])
                            (if doi
                              ["date >= ? and date <= ? and subdomain = ? and doi = ?"  [start-date end-date subdomain doi]]
                              ["date >= ? and date <= ? and subdomain = ?"  [start-date end-date subdomain]]))
                          ; No subdomain
                          (if etld
                            (if doi
                              ["date >= ? and date <= ? and etld = ? and doi = ?"  [start-date end-date etld doi]]
                              ["date >= ? and date <= ? and etld = ?"  [start-date end-date etld]])
                            (if doi
                              ["date >= ? and date <= ? and doi = ?"  [start-date end-date doi]]
                              ["date >= ? and date <= ?" [start-date end-date]]))))
        order-clause (str "order by " date-grouping " asc")
        sql-query (vec (concat [(str select-clause " where " where-clause " group by " date-grouping " " order-clause)] where-args))
        result (j/query (db-connection) sql-query)     
        result-with-date (map #(conj % (date-f %)) result)]
    
    {:days result-with-date :count (reduce + (map :count result-with-date))}))
 

(defn ra-stats [the-query] nil
  ; TODO
  )

(defn query-top-domains [start-date end-date subdomain-rollup ignore-tld page-number page-size]
  "For a given time period return the top domains (i.e. combination of domain and TLD), one-month granularity.
  subdomain-rollup can be one of [nil :all :day :month :year], in which case the subdomains for each domain are found and then optionally grouped in date-buckets over the time period.
  Group by domain.tld or just domain."
  [start-date end-date subdomain-rollup ignore-tld page-number page-size] 
  {:pre [start-date end-date page-number page-size (contains? [nil :all :day :month :year] subdomain-rollup)]}
  
  (let [query (if ignore-tld
        (j/query (db-connection)
          ["select domain, count(domain) as count from resolutions_aggregate where date > ? and date < ? group by domain order by count desc limit ? offset ?" (to-date start-date) (to-date end-date) page-size (* page-size page-number)])
        (j/query (db-connection)
          ["select domain, subdomain, count(domain) as count from resolutions_aggregate where date > ? and date < ? group by domain, subdomain order by count desc limit ? offset ?" (to-date start-date) (to-date end-date) page-size (* page-size page-number)]))
        f (if ignore-tld
            (fn [e] {:full-domain (:domain e) :count (:count e)})
            (fn [e] {:full-domain (str (:subdomain e) (:domain e)) :count (:count e)}))]
    (map f query)))