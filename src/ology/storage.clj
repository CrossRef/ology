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
    (:use [clojure.tools.logging :only (info error)])
    
    )

(let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300 :keepGoing true)
      ^ServerAddress sa  (mg/server-address "127.0.0.1" 27017)]
  (mg/connect! sa opts)
)

; Ignore server errors, continue inserting on error i.e. drop duplicates.
; (mg/set-default-write-concern! WriteConcern/ACKNOWLEDGED)

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
(def tally :c) ; Not used here but used in the javascript map reduce.
(def year-field :y)
(def month-field :m)
(def day-field :a)
(def count-field :count)

(defn $ [a] (str "$" (name a)))

; In the aggregation, some fields are nested under _id.
(defn _id [a] (str "_id." (name a)))

(mg/set-db! (mg/get-db "referral-logs"))

; TODO the entries table isn't being used currently. Remove.
; (defn drop-log-index [] 
;   (try
;     (mc/drop-index "entries" "h_1_i_1_d_1_o_1")
;     ; If there's no index there already, fail silently.
;     (catch com.mongodb.CommandFailureException _ nil)))

; ;
; (defn ensure-log-index []
;   ; Create an index on each field except referrer.
;   (mc/ensure-index "entries" (array-map ip-address 1))
;   (mc/ensure-index "entries" (array-map date-field 1))
;   (mc/ensure-index "entries" (array-map doi-field 1))
;   (mc/ensure-index "entries" (array-map subdomain-field 1))
;   (mc/ensure-index "entries" (array-map domain-field 1))
;   (mc/ensure-index "entries" (array-map tld 1)) 
;   (mc/ensure-index "entries" (array-map followup-ra 1))
  
  ; Create a unique index on the hash, with other bits (except the referrer url which might be massive).
  ; This index will be dropped and re-added to catch duplicates.
  ; (mc/ensure-index "entries" (array-map hashed 1 ip-address 1 date-field 1 doi 1) {:unique true "dropDups" true :sparse true}))

(defn insert-log-entry [entry]
  (mc/update "entries-intermediate" entry entry :upsert true))

(defn insert-log-entries [entries]
  (mc/insert-batch "entries-intermediate" entries))

(defn create-intermediate-collection
  "Drop and create intermediate collection."
  []
  (mc/drop "entries-intermediate")
  
  ; No need to set indexes on this. We do a bulk insert and then iterate inside Mongo.
  ; Index will be set later after insertion and before the aggregation.
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
  [start-start-of-day end-end-of-day]))

(defn clear-aggregates-for-date-range
  "Clear the aggregate table for date range of the intermediate collection."
  [start end]
  ; NB The structure of the date, "_id.dt" vs "dt" depends on whether the map-reduce or aggregation is used.
  ; <= rather than < because the end value is end-of-day for that date.
  (info (str "Remove " (mc/count "entries-aggregated-day" {date-field {"$gte" start "$lte" end}})))
  (mc/remove "entries-aggregated-day" {date-field {"$gte" start "$lte" end}}))


;http://stackoverflow.com/questions/12030322
; TODO this can stack overflow
(defn date-interval
  ([start end] (date-interval start end []))
  ([start end interval]
   (if (time/after? start end)
     interval
     (recur (time/plus start (time/days 1)) end (concat interval [start])))))

(defn query-days
  "For a query, return a vector of the count per day and the total for the whole period."
  [query]
  (let [start-date (:start-date query)
        end-date (:end-date query)
        days (date-interval start-date end-date)
        subdomain (:subdomain query)
        domain (:domain query)
        tld (:tld query)
        doi (:doi query)
        
        count-for-day (fn [day]
          (let [
            match-arg-fns [
            (fn [] (when (and start-date end-date) [["_id.y" (time/year day)] ["_id.m" (time/month day)] ["_id.a" (time/day day)]]))
            (fn [] (when (not (empty? doi)) [[(_id doi-field) doi]]))
            (fn [] (when (not (empty? subdomain)) [[(_id subdomain-field) subdomain]]))
            (fn [] (when (not (empty? domain)) [[(_id domain-field) domain]]))
            (fn [] (when (not (empty? tld)) [[(_id tld-field) tld]]))]
            
            ; Build query map.
            match-args (apply concat (remove nil? ((apply juxt match-arg-fns))))
            ; match (reduce (fn [acc [k v]] (assoc acc k v)) {} match-args)
            match (into {} match-args)
            
            count-response (mc/aggregate "entries-aggregated-day" [
              {"$match" match}
              {"$group" {"_id" "null" count-field {"$sum" ($ count-field)}}}                                                     
            ])
            
            find-response (mc/count "entries-aggregated-day" match)
          ]      
          
          (or (count-field (first count-response)) 0)))
        
          ; Count for every day in the range.
          days-result (map count-for-day days)
          
          ; Total count.
          days-count (reduce + days-result)
        ]
      {:days days-result count-field days-count}))



; Updating the table

(defn update-aggregates-group-partitioned
  "Update aggregates from the intermediate collection. Uses the aggregate/group functionality."
  [start end dois]
  
  ; TODO not used because it lacks the flexibility.

  (info "Indexing intermediate collection")  
  (mc/ensure-index "entries-intermediate" (array-map date-field 1 doi-field 1))

  (info (str "Performing aggregation with " (* (count (date-interval start end)) (count dois)) " steps"))
    
  ; Filter into DOIs per day to keep the aggregation pipline the right size.
  ; The group operation requires loading the entire set into memory.
  ; TODO if this approach is fast enough for real data (splitting on 2 dimensions) then the group can be simplified.
  (doseq [day (date-interval start end)
          the-doi dois]
    
    (let [results
      (mc/aggregate "entries-intermediate" [
      {"$match" {
        date-field {"$gt" (start-of-day day) 
             "$lt" (end-of-day day)}  
        doi-field the-doi
       }}
      ; Carry through the actual date and the date in numbers for ease of querying later.
      {"$project" {
                   doi-field 1
                   subdomain-field 1
                   domain-field 1
                   tld-field 1
                   date-field 1
                   year-field {"$year" ($ date-field)}
                   month-field {"$month" ($ date-field)}
                   day-field {"$dayOfMonth" ($ date-field)}
                   }}
      {"$group" {
            "_id" {
                doi-field ($ doi-field)
                subdomain-field ($ subdomain-field)
                domain-field ($ domain-field)
                tld-field ($ tld-field)
                year-field ($ year-field)
                month-field ($ month-field)
                day-field ($ day-field)
            }
            count-field {"$sum" 1}
            ; The group will collect dates on a given day. This will take an arbitrary date from that day.
            ; The precise value doesn't really matter.
            date-field {"$first" ($ date-field)}
            }}])]
    
    ; Depending on the initial filtering, there should be only one result, or a small number
    ; We can't upsert batches, so iterate over the small range.
    
    (doseq [result results]
      (mc/save "entries-aggregated-day" result)))))


(defn update-aggregates-count-partitioned
  "Update aggregates from the intermediate collection. Filter then count functionality."
  [start end dois]
  
  (info "Indexing intermediate collection")  
  (mc/ensure-index "entries-intermediate" (array-map date-field 1 doi-field 1))
  
  (info (str "Performing aggregation with " (* (count (date-interval start end)) (count dois)) " steps"))
  
  ; TODO: not used as it's not as flexible as group with subdomains.
  ; Filter into DOIs per day to keep the aggregation pipline the right size.
  ; The group operation requires loading the entire set into memory.
  (doseq [the-day (date-interval start end)
          doi dois]
    ; (prn "Update aggregate for" day (start-of-day the-day) "and" doi)
    
    (let [the-count
      (mc/count "entries-intermediate" {
        "d" {"$gt" (start-of-day the-day) 
             "$lt" (end-of-day the-day)}  
        "o" doi
       })
      result {
                "o" doi
                "y" (time/year the-day)
                "m" (time/month the-day)
                "d" (time/day the-day)
                "dt" the-day
                }
              ]
      (mc/insert "entries-aggregated-day" result)
      )))
      

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
