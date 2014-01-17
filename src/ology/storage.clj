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

(def aggregate-table "entries-aggregated-day")

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
(mc/ensure-index aggregate-table (array-map
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
(mc/ensure-index aggregate-table (array-map
  (_id doi-field) 1
  (_id subdomain-field) 1
  (_id domain-field) 1
  (_id tld-field) 1
  date-field 1
))

; Date + DOI
(mc/ensure-index aggregate-table (array-map
  (_id doi-field) 1
  date-field 1
))

; Date + Domain
(mc/ensure-index aggregate-table (array-map
  (_id subdomain-field) 1
  (_id domain-field) 1
  (_id tld-field) 1
  date-field 1
))

(defn query-days
  "For a query, return a vector of the count per day and the total for the whole period."
  [query]
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
        
        match (into {} match-args)
          
        response (mc/aggregate aggregate-table [
          {"$match" match}
          {"$project" {
            "_id" 0
            count-field 1
            year-field ($ (_id year-field))
            month-field ($ (_id month-field))
            day-field ($ (_id day-field))
            date-field ($ date-field)
            }}
          {"$group" {
            "_id" {
              year-field ($ year-field)
              month-field ($ month-field)
              day-field ($ day-field)
            }
          count-field {"$sum" ($ count-field)}
          }}])
          
        total-count (reduce + (map #(:count %1) response))
        
        ; Construct a date object for each day group.
        the-response (map (fn [x] {
          :count (count-field x)
          :date (let [the-date (:_id x)] (time/date-time (year-field the-date) (month-field the-date) (day-field the-date)))})
          response)
        sorted-response (sort-by :date the-response)]
    
      {:days sorted-response :count total-count}))      

; Updating the table

(defn insert-freqs
  "For a map of {[doi, domain-triple] count} and a date, increment the counts in the aggregate table."
  [frequencies the-date]
  (let
    [the-year (time/year the-date)
    the-month (time/month the-date)
    the-day (time/day the-date)]
    
   (doseq [[[doi [subdomain domain tld]] the-count] frequencies]
      (mc/update aggregate-table
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
        :upsert true))))

