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

(set-db! (mg/get-db "referral-logs"))

; Index DOIs uniquely by DOI.


(defn drop-log-index [] 
  (try
    (drop-index "entries" "hash_1_ip_1_date_1_doi_1")
    ; If there's no index there already, fail silently.
    (catch com.mongodb.CommandFailureException _ nil))
)

;
(defn ensure-log-index []
  ; Create an index on each field except referrer.
  (ensure-index "entries" (array-map :ip 1))
  (ensure-index "entries" (array-map :date 1))
  (ensure-index "entries" (array-map :doi 1))
  (ensure-index "entries" (array-map :subdomains 1))
  (ensure-index "entries" (array-map :domain 1))
  (ensure-index "entries" (array-map :tld 1))
  
  ; Create a unique index on the hash, with other bits (except the referrer url which might be massive).
  ; This index will be dropped and re-added to catch duplicates.
  (ensure-index "entries" (array-map :hash 1 :ip 1 :date 1 :doi 1) {:unique true "dropDups" true})
)

(defn insert-log-entry [entry]
  (update "entries" entry entry :upsert true)
)

(defn insert-log-entries [entries]
  (insert-batch "entries" entries)
)