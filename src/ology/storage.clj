(ns ology.storage
    (:gen-class)
    (:require [monger.core :as mg])
    (:import
      [com.mongodb MongoOptions ServerAddress]
      [org.bson.types ObjectId] [com.mongodb DB WriteConcern])
    (:use
      [monger.core :only [connect! connect set-db! get-db]]
      [monger.collection :only [insert insert-batch ensure-index drop-index update]])
    (:require monger.joda-time))

(let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300 :keepGoing true)
      ^ServerAddress sa  (mg/server-address "127.0.0.1" 27017)]
  (connect! sa opts)
)

; Ignore server errors, continue inserting on error i.e. drop duplicates.
(mg/set-default-write-concern! (new WriteConcern 0 1000 false false true))

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

(set-db! (mg/get-db "referral-logs"))

; Index DOIs uniquely by DOI.

(defn drop-log-index [] 
  (try
    (drop-index "entries" "h_1_i_1_d_1_o_1")
    ; If there's no index there already, fail silently.
    (catch com.mongodb.CommandFailureException _ nil)))

;
(defn ensure-log-index []
  ; Create an index on each field except referrer.
  (ensure-index "entries" (array-map ip-address 1))
  (ensure-index "entries" (array-map date 1))
  (ensure-index "entries" (array-map doi 1))
  (ensure-index "entries" (array-map subdomains 1))
  (ensure-index "entries" (array-map domain 1))
  (ensure-index "entries" (array-map tld 1)) 
  (ensure-index "entries" (array-map followup-ra 1))
  
  ; Create a unique index on the hash, with other bits (except the referrer url which might be massive).
  ; This index will be dropped and re-added to catch duplicates.
  (ensure-index "entries" (array-map hashed 1 ip-address 1 date 1 doi 1) {:unique true "dropDups" true :sparse true}))

(defn insert-log-entry [entry]
  (update "entries" entry entry :upsert true))

(defn insert-log-entries [entries]
  (insert-batch "entries" entries))