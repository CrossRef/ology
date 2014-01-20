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
    (:use [clojure.tools.logging :only (info error)]))

(let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300 :keepGoing true)
      ^ServerAddress sa  (mg/server-address "127.0.0.1" 27017)]
  (mg/connect! sa opts))

; Three tables. Data is sufficiently big that although all jobs can be done by the full table it's worth pre-calculating others.
(def aggregate-doi-domain-table "aggregated-domain-doi")
(def aggregate-domain-table "aggregated-domain")
(def aggregate-doi-table "aggregated-doi")

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

; Index for insertion update query.
(mc/ensure-index aggregate-doi-domain-table (array-map
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
(mc/ensure-index aggregate-doi-domain-table (array-map
  (_id doi-field) 1
  (_id subdomain-field) 1
  (_id domain-field) 1
  (_id tld-field) 1
  date-field 1
))

; Date + DOI
(mc/ensure-index aggregate-doi-domain-table (array-map
  (_id doi-field) 1
  date-field 1
))

; Date + Domain
(mc/ensure-index aggregate-doi-domain-table (array-map
  (_id subdomain-field) 1
  (_id domain-field) 1
  (_id tld-field) 1
  date-field 1
))

; Date + DOI
(mc/ensure-index aggregate-doi-table (array-map
  (_id doi-field) 1
  date-field 1
))

; Date + Domain
(mc/ensure-index aggregate-domain-table (array-map
  (_id subdomain-field) 1
  (_id domain-field) 1
  (_id tld-field) 1
  date-field 1
))


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
          (fn [] (when (not (empty? doi)) [[(_id doi-field) doi]]))
          (fn [] (when (not (empty? subdomain)) [[(_id subdomain-field) subdomain]]))
          (fn [] (when (not (empty? domain)) [[(_id domain-field) domain]]))
          (fn [] (when (not (empty? tld)) [[(_id tld-field) tld]]))]
          
        ; Build query map.
        match-args (apply concat (remove nil? ((apply juxt match-arg-fns))))
        
        match-q (into {} match-args)
        
        ; Choose the best collection to run this over.
        table-choice (cond
          (and (get match-q (_id doi-field)) (or (get match-q (_id subdomain-field)) (get match-q (_id domain-field)) (get match-q (_id tld-field)))) aggregate-doi-domain-table
          (get match-q (_id doi-field)) aggregate-doi-table
          (or (match-q (_id subdomain-field)) (get match-q (_id domain-field)) (get match-q (_id tld-field))) aggregate-domain-table
          :else aggregate-doi-domain-table)
        
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
            year-field ($ (_id year-field))
            month-field ($ (_id month-field))
            day-field ($ (_id day-field))
            date-field ($ date-field)}}
          {"$group" {
            "_id" group-q          
            count-field {"$sum" ($ count-field)}}}
          ])
          
        total-count (reduce + (map #(:count %1) response))
        
        ; Construct a date object for each day group.
        the-response (map (fn [x] {:count (count-field x) :date (build-date-f (:_id x))}) response)
        sorted-response (sort-by :date the-response)
        ]
    
      (info "Query days using collection " table-choice)    
        {:days sorted-response :count total-count}))

(defn query-top-domains
  "For a given time period return the top domains (i.e. combination of domain and TLD)."
  [start-date end-date]
  (let [response (mc/aggregate aggregate-domain-table [
    {"$match" {date-field {"$gte" start-date "$lte" end-date}}}
    {"$project" {
            "_id" 0
            count-field ($ count-field)
            subdomain-field ($ (_id subdomain-field))
            domain-field ($ (_id domain-field))
            tld-field ($ (_id tld-field))
            date-field ($ date-field)}}
    {"$group"
     {"_id" {domain-field ($ domain-field) tld-field ($ tld-field)}
      "count" {"$sum" ($ count-field)}
      }}
    ; Exclude small counts. Arbitrary number.
    {"$match" {count-field {"$gt" 10}}}
    {"$sort" {count-field -1}}
  ])] response))

; Updating the table

(defn insert-freqs
  "For a map of {[doi, domain-triple] count} and a date, increment the counts in the aggregate table."
  [frequencies the-date]
  (let
    [the-year (time/year the-date)
    the-month (time/month the-date)
    the-day (time/day the-date)]
    
   (doseq [[[doi [subdomain domain tld]] the-count] frequencies]
     ; Update the DOI x domain x date table.
      (mc/update aggregate-doi-domain-table
        {:_id {
          doi-field doi
          year-field the-year
          month-field the-month
          day-field the-day
          subdomain-field subdomain
          domain-field domain
          tld-field tld
        }}
        {"$inc" {count-field the-count}
         "$set" {date-field the-date}}
        :upsert true)

      ; Update domain x date
      (mc/update aggregate-domain-table
        {:_id {
          year-field the-year
          month-field the-month
          day-field the-day
          subdomain-field subdomain
          domain-field domain
          tld-field tld
        }}
        {"$inc" {count-field the-count}
         "$set" {date-field the-date}}
        :upsert true)
      
      ; Update DOI x date
      (mc/update aggregate-doi-table
        {:_id {
          year-field the-year
          month-field the-month
          day-field the-day
          doi-field doi
        }}
        {"$inc" {count-field the-count}
         "$set" {date-field the-date}}
        :upsert true)
      
      )
   
   ))

