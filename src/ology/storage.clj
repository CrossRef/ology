(ns ology.storage
    (:gen-class)
    (:require [monger.core :as mg])
    (:import
      [com.mongodb MongoOptions ServerAddress]
      [org.bson.types ObjectId] [com.mongodb DB WriteConcern])
    (:use
      [monger.core :only [connect! connect set-db! get-db]]
      [monger.collection :only [insert insert-batch ensure-index drop-indexes update]])
    (:require monger.joda-time))

(let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300 :keepGoing true) ; todo remove keepgoing
      ^ServerAddress sa  (mg/server-address "127.0.0.1" 27017)]

(connect! sa opts)

)

(set-db! (mg/get-db "referral-logs"))

; Index DOIs uniquely by DOI.


(defn drop-log-index [] (drop-indexes "entries"))

(defn ensure-log-index [] (ensure-index "entries" (array-map :ip 1
:date 1
:doi 1
:referrer 1
:subdomains 1
:domain 1
:tld 1) {:unique true :dropDups true}))

(defn insert-log-entry [entry]
  (update "entries" entry entry :upsert true)
)

(defn insert-log-entries [entries]
  (insert-batch "entries" entries)
)