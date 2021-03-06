(ns ology.storage
    (:gen-class)
    (:import
      [com.mongodb MongoOptions ServerAddress]
      [com.mongodb MapReduceCommand$OutputType MapReduceOutput]
      [org.bson.types ObjectId] [com.mongodb DB WriteConcern])
    (:require
      [monger.core :as mg]
      [monger.collection :as mc])
    (:require monger.joda-time)
    (:require [clj-time.core :as time])
    (:require [clj-http.client :as client])
    (:use [clojure.tools.logging :only (info error)]))

(let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300 :keepGoing true)
      ^ServerAddress sa  (mg/server-address "127.0.0.1" 27017)]
  (mg/connect! sa opts))


(def batch-size 10000)

; Three tables. Data is sufficiently big that although all jobs can be done by the full table it's worth pre-calculating others.
(def aggregate-doi-domain-table-day "aggregated-domain-doi-day")
(def aggregate-domain-table-day "aggregated-domain-day")
(def aggregate-doi-table-day "aggregated-doi-day")

(def aggregate-doi-domain-table-month "aggregated-domain-doi-month")
(def aggregate-domain-table-month "aggregated-domain-month")
(def aggregate-doi-table-month "aggregated-doi-month")


; Short field names for storing in Mongo.
(def ip-address :i)
(def date-field :d)
(def doi-field :o)
(def subdomain-field :s)
(def domain-field :n)
(def tld-field :l)
(def hashed :h)
(def followup-ra :f)
(def referrer :r)
(def year-field :y)
(def month-field :m)
(def day-field :a)
(def count-field :count)

(defn $ [a] (str "$" (name a)))

; In the aggregation, some fields are nested under _id.
(defn _id [a] (str "_id." (name a)))

(mg/set-db! (mg/get-db "referral-logs"))
(mg/set-default-write-concern! WriteConcern/UNACKNOWLEDGED)

(when false
  ; Index for insertion update query.
  (mc/ensure-index aggregate-doi-domain-table-day (array-map
    (_id year-field) 1
    (_id month-field) 1
    (_id day-field) 1
    (_id doi-field) 1
    (_id subdomain-field) 1
    (_id domain-field) 1
    (_id tld-field) 1
  ))

  ; Indexes for various API queries.

  ; Date + DOI + domain
  (mc/ensure-index aggregate-doi-domain-table-day (array-map
    (_id doi-field) 1
    (_id subdomain-field) 1
    (_id domain-field) 1
    (_id tld-field) 1
    date-field 1
  ))

  ; Date + DOI
  (mc/ensure-index aggregate-doi-domain-table-day (array-map
    (_id doi-field) 1
    date-field 1
  ))

  ; Date + Domain
  (mc/ensure-index aggregate-doi-domain-table-day (array-map
    (_id subdomain-field) 1
    (_id domain-field) 1
    (_id tld-field) 1
    date-field 1
  ))

  ; Date + DOI
  (mc/ensure-index aggregate-doi-table-day (array-map
    (_id doi-field) 1
    date-field 1
  ))

  ; Date + Domain
  (mc/ensure-index aggregate-domain-table-day (array-map
    (_id subdomain-field) 1
    (_id domain-field) 1
    (_id tld-field) 1
    date-field 1
  ))
)

(mc/drop-indexes aggregate-doi-domain-table-day)
(mc/drop-indexes aggregate-doi-table-day)
(mc/drop-indexes aggregate-domain-table-day)
(mc/drop-indexes aggregate-doi-domain-table-month)
(mc/drop-indexes aggregate-doi-table-month)
(mc/drop-indexes aggregate-domain-table-month)


(def doi-registration-authority-service-url "http://192.168.1.14:8000/ra/")

(defn lookup-ra
  "Talk to the RA webservice, return status as one of :crossref :other-ra :does-not-exist :invalid :not-available"
  [doi]
  (try
      (let
        [response (client/get (str doi-registration-authority-service-url doi) {:accept :json :as :json})]
        (if (= (-> response :body first :status) "Invalid DOI")
          :invalid
          (case (-> response :body first :RA)
            nil :not-available
            "CrossRef" :crossref
            "DOI does not exist" :does-not-exist
            :other-ra
          )))
        (catch Exception _ :not-available)))


(defn ra-stats
  "Calculate proportion of CrossRef vs non-CrossRef RA DOIs. Month granularity."
  [query]
  (let [start-date (:start-date query)
        end-date (:end-date query)
        domain (:domain query)
        tld (:tld query)
        query (mc/find aggregate-doi-domain-table-month {date-field {"$gte" start-date "$lte" end-date} domain-field domain tld-field tld} [doi-field "count"])
        stats (reduce (fn [[crossref non-crossref] entry]
         (if (= :crossref (lookup-ra (get entry "doi")))
         [(+ crossref (get entry "count")) non-crossref]
         [non-crossref (+ non-crossref (get entry "count"))]))
         [0 0] query)]
    {:crossref (get stats 0) :non-crossref (get stats 1)}))

(defn query-days
  "For a query, return a vector of the count per day and the total for the whole period. Group by argument should be one of [:day :month :year]"
  [query group-by]
  (let [start-date (:start-date query)
        end-date (:end-date query)
        subdomain (:subdomain query)
        domain (:domain query)
        tld (:tld query)
        doi (:doi query)

        match-arg-fns [
          (fn [] (when (and start-date end-date) [["d" {"$gte" start-date "$lte" end-date}]]))
          (fn [] (when (not (empty? doi)) [[doi-field doi]]))
          (fn [] (when (not (empty? subdomain)) [[subdomain-field subdomain]]))
          (fn [] (when (not (empty? domain)) [[domain-field domain]]))
          (fn [] (when (not (empty? tld)) [[tld-field tld]]))]
          
        ; Build query map.
        match-args (apply concat (remove nil? ((apply juxt match-arg-fns))))
        
        match-q (into {} match-args)
        
        ; Choose the best collection to run this over. Any table will do, but the data in each of the six is pre-optimised.
        table-choice (cond
          
          ; DOI and Domain, per day.
          (and
            (= group-by "day")
            (get match-q doi-field)
            (or (get match-q subdomain-field)
                (get match-q domain-field)
                (get match-q tld-field)))
          aggregate-doi-domain-table-day

          ;; DOI and domain, per month or year.
          (and
            (or (= group-by "month") (= group-by "year"))
            (get match-q doi-field)
            (or (get match-q subdomain-field)
                (get match-q domain-field)
                (get match-q tld-field)))
          aggregate-doi-domain-table-month
                    
          ; DOI per day.
          (and
            (= group-by "day")
            (get match-q doi-field))
          aggregate-doi-table-month

          ; DOI per month or year.
          (and
            (or (= group-by "month") (= group-by "year"))
            (get match-q doi-field))
          aggregate-doi-table-day
                    
          ; DOI per day.
          (and 
            (= group-by "day")
            (or
              (match-q subdomain-field)
              (get match-q domain-field)
              (get match-q tld-field)))
          aggregate-domain-table-day

          ; DOI per month or year.
          (and 
            (or (= group-by "month") (= group-by "year"))
            (or
              (match-q subdomain-field)
              (get match-q domain-field)
              (get match-q tld-field)))
          aggregate-domain-table-month
          
          ; Or the big table.
          :else aggregate-doi-domain-table-day)
        
        group-q (cond
          (= group-by "year") {year-field ($ year-field)}
          (= group-by "month") {year-field ($ year-field) month-field ($ month-field)}
          (= group-by "day") {year-field ($ year-field) month-field ($ month-field) day-field ($ day-field)}

          ; Default by day.
          :else {year-field ($ year-field) month-field ($ month-field) day-field ($ day-field)})
        
        ; A function to construct the date out of the limited result of the group aggregation.
        build-date-f (cond
          (= group-by "year") (fn [x] (time/date-time (year-field x)))
          (= group-by "month") (fn [x] (time/date-time (year-field x) (month-field x)))
          (= group-by "day") (fn [x] (time/date-time (year-field x) (month-field x) (day-field x))

          ; Default by day.
          :else (time/date-time (year-field x) (month-field x) (day-field x))))
          
        response (mc/aggregate table-choice [
          {"$match" match-q}
          {"$project" {
            "_id" 0
            count-field 1
            year-field ($ year-field)
            month-field ($ month-field)
            day-field ($ day-field)
            date-field ($ date-field)}}
          {"$group" {
            "_id" group-q          
            count-field {"$sum" ($ count-field)}}}
          {"$project" {
            "_id" 0
            count-field 1
            year-field ($ (_id year-field))
            month-field ($ (_id month-field))
            day-field ($ (_id day-field))}}
          ])
          
        total-count (reduce + (map #(:count %1) response))
        
        ; Construct a date object for each day group.
        the-response (map (fn [x] {:count (count-field x) :date (build-date-f x)}) response)
        sorted-response (sort-by :date the-response)
        ]
      
      (info "Query days using collection " table-choice)    
        {:days sorted-response :count total-count}))

(defn query-top-domains
  "For a given time period return the top domains (i.e. combination of domain and TLD), one-month granularity.
  subdomain-rollup can be one of [nil :all :day :month :year], in which case the subdomains for each domain are found and then optionally grouped in date-buckets over the time period.
  Group by domain.tld or just domain.
  "
  [start-date end-date subdomain-rollup ignore-tld page-number page-size] 
  (let [response (mc/aggregate aggregate-domain-table-month [
    {"$match" {date-field {"$gte" start-date "$lte" end-date}}}
   
    {"$group"
     {"_id" (if ignore-tld
              {domain-field ($ domain-field)}
              {domain-field ($ domain-field) tld-field ($ tld-field)})
      "count" {"$sum" ($ count-field)}
      }}
    ; Exclude small counts. Arbitrary number.
    {"$match" {count-field {"$gt" 10}}}
    
    ; Mongo has some sorting magic for this sequence of operations.
    ; http://docs.mongodb.org/manual/reference/operator/aggregation/limit/
    {"$sort" {count-field -1}}
    {"$limit" (* (inc page-number) page-size)}
    {"$skip" (- (* (inc page-number) page-size) page-size)}
  ])
        
    response-formatted (map (fn [entry] {:full-domain (str (domain-field (:_id entry)) "." (tld-field (:_id entry))) :count (:count entry) domain-field (domain-field (:_id entry)) tld-field (tld-field (:_id entry))}) response)    
  ]
    (if (not subdomain-rollup)
      response-formatted 
      (map (fn [entry]
             
        (let [; Choose a Mongo aggregation group function.
            rollup-group-q (case subdomain-rollup
            :all {"$group" {"_id" {domain-field ($ domain-field) tld-field ($ tld-field) subdomain-field ($ subdomain-field)} "count" {"$sum" ($ count-field)}}}
            :day {"$group" {"_id" {domain-field ($ domain-field) tld-field ($ tld-field) subdomain-field ($ subdomain-field) year-field ($ year-field) month-field ($ month-field) day-field ($ day-field)} "count" {"$sum" ($ count-field)}}}
            :month {"$group" {"_id" {domain-field ($ domain-field) tld-field ($ tld-field) subdomain-field ($ subdomain-field) year-field ($ year-field) month-field ($ month-field)} "count" {"$sum" ($ count-field)}}}
            :year {"$group" {"_id" {domain-field ($ domain-field) tld-field ($ tld-field) subdomain-field ($ subdomain-field) year-field ($ year-field)} "count" {"$sum" ($ count-field)}}}
            {"$group" {"_id" {domain-field ($ domain-field) tld-field ($ tld-field) subdomain-field ($ subdomain-field)} "count" {"$sum" ($ count-field)}}}
            ) 
            
            ; Choose a function to format turn Mongo rollup entries.
            rollup-format-f (case subdomain-rollup
              :all (fn [entry] {
                :full-domain (str (subdomain-field (:_id entry)) "." (domain-field (:_id entry)) "." (tld-field (:_id entry)))
                :count (:count entry)})
              :day (fn [entry] {
                :full-domain (str (subdomain-field (:_id entry)) "." (domain-field (:_id entry)) "." (tld-field (:_id entry)))
                :count (:count entry)
                :date (time/date-time (year-field (:_id entry)) (month-field (:_id entry)) (day-field (:_id entry)))
                :year (year-field (:_id entry))
                :month (month-field (:_id entry))
                :day (day-field (:_id entry))
                })
              :month (fn [entry] {
                :full-domain (str (subdomain-field (:_id entry)) "." (domain-field (:_id entry)) "." (tld-field (:_id entry)))
                :count (:count entry)
                :date (time/date-time (year-field (:_id entry)) (month-field (:_id entry)))
                :year (year-field (:_id entry))
                :month (month-field (:_id entry))
                })
              :year (fn [entry]
                {:full-domain (str (subdomain-field (:_id entry)) "." (domain-field (:_id entry)) "." (tld-field (:_id entry)))
                 :count (:count entry)
                 :date (time/date-time (year-field (:_id entry)))
                 :year (year-field (:_id entry))
                 })
              (fn [entry]
                {:full-domain (str (subdomain-field (:_id entry)) "." (domain-field (:_id entry)) "." (tld-field (:_id entry))) :count (:count entry)
                 :date start-date
                 })                  
            )
            subdomain-response 
        (mc/aggregate aggregate-domain-table-day [
          {"$match" (if ignore-tld
            {date-field {"$gte" start-date "$lte" end-date} 
             domain-field (domain-field entry)}
            {date-field {"$gte" start-date "$lte" end-date} 
             domain-field (domain-field entry)
             tld-field (tld-field entry)})}
          
          ; Group aggregation pipeline function.
          rollup-group-q
          
          ; Exclude small counts. Arbitrary number.
          {"$match" {count-field {"$gt" 10}}}
          {"$sort" {count-field -1}}
        ])
        ; Format and sort buckets per subdomain by date.
        subdomain-response-formatted (sort-by :date (map rollup-format-f subdomain-response))
        ; Group into per subdomain, turning the group map into a vector.
        subdomain-date-group (map first (map second (group-by :full-domain subdomain-response-formatted)))
        subdomain-date-group-sorted (reverse (sort-by :count subdomain-date-group))
        ]
        ; Output from map function.
        {:domain entry :subdomains subdomain-date-group-sorted})       
      ) response-formatted))))

; Updating the table

; Strange, but this is currently the quicket way of inserting documents.
; https://groups.google.com/forum/#!msg/clojure-mongodb/1gxBRWOYF3o/IitJZyrBg34J

(defn insert-doi-freqs
  "For a map of {[doi, domain-triple] count} and a date, increment the counts in the aggregate table. period-type is one of [:day :month]"
  [frequencies the-date period-type]
  (let
    [the-year (time/year the-date)
    the-month (time/month the-date)
    the-day (time/day the-date)
                    
    doi-values (map (fn [[doi the-count]]
      { year-field the-year
        month-field the-month
        day-field the-day
        doi-field doi
        count-field the-count
        date-field the-date}) frequencies)]
  
    (doseq [batch (partition batch-size batch-size nil doi-values)]
      (when (= period-type :day)
      (dorun (map #(mc/insert-and-return aggregate-doi-table-day % WriteConcern/UNACKNOWLEDGED) batch)))
      
      (when (= period-type :month)
      (dorun (map #(mc/insert-and-return aggregate-doi-table-month % WriteConcern/UNACKNOWLEDGED) batch))))))

(defn insert-domain-freqs
  "For a map of {[doi, domain-triple] count} and a date, increment the counts in the aggregate table. period-type is one of [:day :month]"
  [frequencies the-date period-type]
  (let
    [the-year (time/year the-date)
    the-month (time/month the-date)
    the-day (time/day the-date)

     domain-values (map (fn [[[subdomain domain tld] the-count]] 
      { year-field the-year
        month-field the-month
        day-field the-day
        subdomain-field subdomain
        domain-field domain
        tld-field tld
        count-field the-count
        date-field the-date}) frequencies)]
  
    (doseq [batch (partition batch-size batch-size nil domain-values)]
      (when (= period-type :day)
      (dorun (map #(mc/insert-and-return aggregate-domain-table-day % WriteConcern/UNACKNOWLEDGED) batch)))
      
      (when (= period-type :month)
      (dorun (map #(mc/insert-and-return aggregate-domain-table-month % WriteConcern/UNACKNOWLEDGED) batch))))))

(defn insert-domain-doi-freqs
  "For a map of {[doi, domain-triple] count} and a date, increment the counts in the aggregate table. period-type is one of [:day :month]"
  [frequencies the-date period-type]
  (let
    [the-year (time/year the-date)
    the-month (time/month the-date)
    the-day (time/day the-date)
    
    doi-domain-values (map (fn [[[doi [subdomain domain tld]] the-count]]      
      { doi-field doi
        year-field the-year
        month-field the-month
        day-field the-day
        subdomain-field subdomain
        domain-field domain
        tld-field tld
        count-field the-count
        date-field the-date}
        ) frequencies)
    ]
  
    (doseq [batch (partition batch-size batch-size nil doi-domain-values)]
      (when (= period-type :day)
      (dorun (map #(mc/insert-and-return aggregate-doi-domain-table-day % WriteConcern/UNACKNOWLEDGED) batch)))

      (when (= period-type :month)
      (dorun (map #(mc/insert-and-return aggregate-doi-domain-table-month % WriteConcern/UNACKNOWLEDGED) batch))))))

