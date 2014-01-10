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
    
    
    )

(let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300 :keepGoing true)
      ^ServerAddress sa  (mg/server-address "127.0.0.1" 27017)]
  (mg/connect! sa opts)
)

; Ignore server errors, continue inserting on error i.e. drop duplicates.
(mg/set-default-write-concern! WriteConcern/ACKNOWLEDGED)

; Short field names for storing in Mongo.
(def ip-address :i)
(def date :d)
(def doi :o)
(def subdomains :s)
(def domain :n)
(def tld :l)
(def hashed :h)
(def followup-ra :f)
(def referrer :r)
(def tally :c) ; Not used here but used in the javascript map reduce.

(mg/set-db! (mg/get-db "referral-logs"))

(defn drop-log-index [] 
  (try
    (mc/drop-index "entries" "h_1_i_1_d_1_o_1")
    ; If there's no index there already, fail silently.
    (catch com.mongodb.CommandFailureException _ nil)))

;
(defn ensure-log-index []
  ; Create an index on each field except referrer.
  (mc/ensure-index "entries" (array-map ip-address 1))
  (mc/ensure-index "entries" (array-map date 1))
  (mc/ensure-index "entries" (array-map doi 1))
  (mc/ensure-index "entries" (array-map subdomains 1))
  (mc/ensure-index "entries" (array-map domain 1))
  (mc/ensure-index "entries" (array-map tld 1)) 
  (mc/ensure-index "entries" (array-map followup-ra 1))
  
  ; Create a unique index on the hash, with other bits (except the referrer url which might be massive).
  ; This index will be dropped and re-added to catch duplicates.
  (mc/ensure-index "entries" (array-map hashed 1 ip-address 1 date 1 doi 1) {:unique true "dropDups" true :sparse true}))

(defn insert-log-entry [entry]
  (mc/update "entries-intermediate" entry entry :upsert true))

(defn insert-log-entries [entries]
  (mc/insert-batch "entries-intermediate" entries))

(defn create-intermediate-collection
  "Drop and create intermediate collection."
  []
  (mc/drop "entries-intermediate")
  
  ; No need to set indexes on this. We do a bulk insert and then iterate inside Mongo.
  (mc/create "entries-intermediate" {})
)

(defn start-of-day [d] (.withTimeAtStartOfDay d))
(defn end-of-day [d] (.withTime d 23 59 59 999))

(defn get-date-range
  "Get range of dates, return start of earliest day, end of latest day."
  []
  (let [results (mc/aggregate "entries-intermediate" [{"$group" {"_id" 0 :min-date {"$min" "$d"} :max-date {"$max" "$d"}}}])
        start (-> results first :min-date)
        end (-> results first :max-date)
        ; The dates in the aggregate table don't have the original times, so generalise these to 'whole day'.
        start-start-of-day (start-of-day start)
        end-end-of-day (end-of-day end)]
    (prn "returning" start-start-of-day end-end-of-day)
  [start-start-of-day end-end-of-day]))

(defn clear-aggregates-for-date-range
  "Clear the aggregate table for date range of the intermediate collection."
  [start end]
  (mc/remove "entries-aggregated-day" {"_id.d" {"$gte" start "$lt" end}})
  (mc/remove "entries-aggregated-week" {"_id.d" {"$gte" start "$lt" end}})
  (mc/remove "entries-aggregated-month" {"_id.d" {"$gte" start "$lt" end}}))


;http://stackoverflow.com/questions/12030322
(defn date-interval
  ([start end] (date-interval start end []))
  ([start end interval]
   (if (time/after? start end)
     interval
     (recur (time/plus start (time/days 1)) end (concat interval [start])))))


(defn update-aggregates-group-partitioned
  "Update aggregates from the intermediate collection. Uses the aggregate/group functionality."
  [start end dois]
  
  ; Filter into DOIs per day to keep the aggregation pipline the right size.
  ; The group operation requires loading the entire set into memory.
  ; TODO if this approach is fast enough for real data (splitting on 2 dimensions) then the group can be simplified.
  (doseq [day (date-interval start end)
          doi dois]
    ; (prn "Update aggregate for" day (start-of-day day) "and" doi)
    
    (let [results
      (mc/aggregate "entries-intermediate" [
      {"$match" {
        "d" {"$gt" (start-of-day day) 
             "$lt" (end-of-day day)}  
        "o" doi
       }}
      ; Carry through the actual date and the date in numbers for ease of querying later.
      {"$project" {
                   "o" "$o"
                   "s" "$s"
                   "n" "$n"
                   "l" "$l"
                   "d" "$d"
                   "date" {"y" {"$year" "$d"}, "m" {"$month" "$d"}, "d" {"$dayOfMonth" "$d"}}}}
      {"$group" {
            "_id" {
                "o" "$o"
                "y" "$date.y"
                "m" "$date.m"
                "d" "$date.d"
                "dt" "$d"
            },
            "count" {"$sum" 1}
      }}])]
    
    ; Depending on the initial filtering, there should be only one result, or a small number
    ; We can't upsert batches, so iterate over the small range.
    (doseq [result results] (mc/update "entries-aggregated-day" result result :upsert true)))))

(defn update-aggregates-group-naive
  "Update aggregates from the intermediate collection. Uses the aggregate/group functionality."
  []
  ; TODO  retired
  ; This doesn't work because for the group to work mongo has to read the entire set into RAM.
  (mc/aggregate "entries-intermediate" [{"$group" {"_id" 0 :min-date {"$min" "$d"} :max-date {"$max" "$d"}}}])
)

(defn update-aggregates-mapreduce
  "Update aggregates from the intermediate collection. Uses map/reduce functionality."
  []
  ; TODO retired. An overnight run on 78 million records managed 3% in 14 hours.
  ; Set the hour to 1, otherwise the date is recorded as midnight and shows up as the previous day.
  (mc/map-reduce
                  "entries-intermediate"
                  "function() {
                    emit({
                      'o': this.o, 's': this.s, 'n': this.n, 'l': this.l,
                      'di': {'y': this.d.getFullYear(), 'm': this.d.getMonth()+1, 'd': this.d.getDate()},
                      'd': new Date(this.d.getFullYear(), this.d.getMonth(), this.d.getDate(), 1)}, 1)}"
                  "function(previous, current) {var count = 0; for (index in current) { count += current[index]; } return count; }"
                  "entries-aggregated-day"
                  MapReduceCommand$OutputType/MERGE {})

  ; http://stackoverflow.com/questions/7765767/
  ; http://stackoverflow.com/questions/16590500/
  (mc/map-reduce
                  "entries-intermediate"
                  "function() {
                    var onejan = new Date(this.d.getFullYear(),0,1);
                    var weekNumber = Math.ceil((((this.d - onejan) / 86400000) + onejan.getDay()+1)/7);
                    var weekDate = new Date(this.d.getFullYear(), 0, (1 + (weekNumber - 1) * 7)); 
                    emit({'o': this.o, 's': this.s, 'n': this.n, 'l': this.l, 'di': {'y': this.d.getFullYear(), 'w': weekNumber}, 'd': weekDate}, 1);
                  }"
                  "function(previous, current) {var count = 0; for (index in current) { count += current[index]; } return count; }"
                  "entries-aggregated-week"
                  MapReduceCommand$OutputType/MERGE {})
  (mc/map-reduce
                  "entries-intermediate"
                  "function() {
                    emit(
                     {'o': this.o,
                      's': this.s, 'n': this.n, 'l': this.l
                      //'di': {'y': this.d.getFullYear(), 'm': this.d.getMonth()+1}, 'd': new Date(this.d.getFullYear(), this.d.getMonth(), 1)
                     },
                  1)
                  }"
                  "function(previous, current) {var count = 0; for (index in current) { count += current[index]; } return count; }"
                  "entries-aggregated-month"
                  MapReduceCommand$OutputType/MERGE {}))
