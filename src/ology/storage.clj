(ns ology.storage
    (:gen-class)
    (:require [monger.core :as mg])
    (:import
      [com.mongodb MongoOptions ServerAddress]
      [org.bson.types ObjectId] [com.mongodb DB WriteConcern])
    (:use
      [monger.core :only [connect! connect set-db! get-db]]
      [monger.collection :only [insert insert-batch ensure-index update]])
    (:require monger.joda-time))

(connect!)
(set-db! (mg/get-db "referral-logs"))

; Index DOIs uniquely by DOI.
(ensure-index "entries" (array-map :ip 1
:date 1
:doi 1
:referrer 1
:subdomains 1
:domain 1
:tld 1) {:unique true })

(defn insert-log-entry [entry]
  (update "entries" entry entry :upsert true)
)