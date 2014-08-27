(ns ology.mysql
    (:require [korma.core :refer [as-sql defentity entity-fields pk table select subselect select* where order insert update values delete exec-raw set-fields fields sql-only join aggregate group with has-one has-many limit offset]])
    (:require [korma.db :refer [defdb mysql]])
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.coerce :refer [to-sql-date]])
    (:require [clj-time.core :refer [date-time]])
    (:require [clojure.java.jdbc :as j])
    (:import com.mchange.v2.c3p0.ComboPooledDataSource)
    (:require [ology.config :refer [config]])
    (:gen-class))

(defdb db (mysql {:db (config :mysql-name)
                       :user (config :mysql-username)
                       :host (or (config :mysql-host) "localhost")
                       :port (or (config :mysql-port) 3306)
                       :password (config :mysql-password)}))

(defentity resolutions
  (table "resolutions")
  (entity-fields :date :doi :subdomain :domain :etld))

(defentity resolutions-date-aggregate
  (table "resolutions_date_aggregate")
  (entity-fields :date :doi :subdomain :domain :etld :y :m :d)) ; :count is excluded as it's always aggregated

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
   (let [entries-safe (map (fn [[date doi subdomain domain etld]] {:date (to-sql-date date) :doi (trunc doi max-doi-length) :subdomain (trunc subdomain max-subdomain-length) :domain (trunc domain max-domain-length) :etld (trunc etld max-etld-length)}) entries)]
  (try
    (when (> (count entries) 0)
      (insert resolutions (values entries-safe)))
  (catch Exception ex (prn "Exception:" ex entries-safe)))))

(defn resolutions-table-size
  []
  (let [result (select resolutions (aggregate (count :*) :count))]
    (-> result first :count)))

(defn aggregated-table-size
  []
  (let [result (select resolutions-date-aggregate (aggregate (count :*) :count))]
    (-> result first :count)))


(defn produce-aggregations
  "For all the data in the resolutions table calculate aggregation and insert into resolution-aggregate table."
  []
  ; TODO this works for 1,000,000 entries but not 1,000,000,000. Need to partition.
  ; TODO maybe rewrite in korma DSL.
  (exec-raw ["insert into resolutions_date_aggregate (count, subdomain, domain, etld, date, doi, y, m, d) select count(date) as count, subdomain, domain, etld, date, doi, extract(year from date), extract(month from date), extract(day from date) from resolutions group by date, subdomain, domain, etld, doi order by date"]))

(defn produce-aggregations-partitioned
  "For all the data in the resolutions table calculate aggregation and insert into resolution-aggregate table."
  []
  (info "Produce aggregations partitioned.")
  (let [page-size 100000
        dates (exec-raw "select distinct(date) from resolutions")]
    (info "Got" (count dates) "dates")
    (doseq [the-date (map :date dates)]
      (info "Aggregate for date" the-date)
        ; This is expensive. Only for debugging.
        ; (info "Found" (-> (j/query (db-connection) ["select count(*) as count from resolutions where date = ?" the-date]) first :count) "entries")
        (info "Doing insert...")
        (info "Size of aggregate table before" (aggregated-table-size))
        (loop [page-num 0]
          (info "date" the-date "page" page-num)
          (let [insert-result (exec-raw ["insert into resolutions_date_aggregate (count, subdomain, domain, etld, date, doi, y, m, d) select count(date) as count, subdomain, domain, etld, date, doi, extract(year from date), extract(month from date), extract(day from date) from resolutions where date = ? group by date, subdomain, domain, etld, doi order by date limit ? offset ?" the-date page-size (* page-size page-num)])
                num-inserted (first insert-result)]
            (info "Inserted: " num-inserted) 
            (info "Size of aggregate table after" (aggregated-table-size))
          
          (when (> num-inserted 0) (recur (inc page-num)))))
        (info "Finished insert."))))

(defn flush-aggregations-table
  []
  (exec-raw "delete from resolutions"))

; 'storage' interface.

(defn empty-string-to-nil [input] (if (= input "") nil input))

(defn query-days
  "For a query, return a vector of the count per day and the total for the whole period."
  [query to-group-by]
  {:pre [(#{"day" "month" "year"} to-group-by)]}
  (let [start-date (to-sql-date (:start-date query))
        end-date (to-sql-date (:end-date query))
        subdomain (empty-string-to-nil (:subdomain query))
        domain (empty-string-to-nil (:domain query))
        etld (empty-string-to-nil (:tld query))
        doi (empty-string-to-nil (:doi query))
        
        q  (select* resolutions-date-aggregate)
        
        ; Filter
        q (where q (>= :date start-date))
        q (where q (<= :date end-date))
        q (if domain (where q (= :domain domain)) q)
        q (if subdomain (where q (= :subdomain subdomain)) q)
        q (if etld (where q (= :etld etld)) q)
        q (if doi (where q (= :doi doi)) q)
        
        ; Aggregate
        q (aggregate q (sum :count) :count)
        
        ; Group
        q (condp = to-group-by "day" (group q :y :m :d) "month" (group q :y :m) "year" (group q :y))
        
        ; q (limit q 10)
                          
        date-f (condp = to-group-by "day" (fn [row] {:date (date-time (:y row) (:m row) (:d row))})
                                    "month" (fn [row] {:date (date-time (:y row) (:m row))})
                                    "year" (fn [row] {:date (date-time (:y row))}))
        
        result (select q)
        result-with-date (map #(conj % (date-f %)) result)]
    
    {:days (select q)
     :count (reduce + (map :count result-with-date))}))
 

(defn ra-stats [the-query] nil
  ; TODO
  )

(defn query-top-domains [start-date end-date subdomain-rollup ignore-tld page-number page-size]
  "For a given time period return the top domains (i.e. combination of domain and TLD), one-month granularity.
  subdomain-rollup can be one of [nil :all :day :month :year], in which case the subdomains for each domain are found and then optionally grouped in date-buckets over the time period.
  Group by domain.tld or just domain."
  [start-date end-date subdomain-rollup ignore-tld page-number page-size] 
  {:pre [start-date end-date page-number page-size (contains? [nil :all :day :month :year] subdomain-rollup)]}
    
  (let [q (-> (select* resolutions-date-aggregate)
              (aggregate (count :domain) :count)
              
              ; (where (>= :date (to-sql-date start-date)))
              ; (where (<= :date (to-sql-date end-date)))
              (order :count :DESC)
              (limit page-size)
              (offset (* page-size page-number))
              )
        
        q (if ignore-tld (fields q [:domain] [:domain :subdomain]))
        
        q (if ignore-tld (group q :domain) (group q :domain :subdomain))
        
          
        result (select q)
        
        f (if ignore-tld
            (fn [e] {:full-domain (:domain e) :count (:count e)})
            (fn [e] {:full-domain (str (:subdomain e) (:domain e)) :count (:count e)}))
          
        with-domain (map f result)]
  
    with-domain))
          
