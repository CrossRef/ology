(ns ology.handler
  (:use compojure.core)
  (:use ology.core)
  (:require [ology.storage :as storage])
  (:require [compojure.handler :as handler]
            [compojure.route :as route])
  (:use ring.middleware.json)
  (:use ring.util.response)
  (:use [clj-time.format])
)

(def date-formatter (formatter "yyyy-MM-dd"))

(defn heartbeat [request]
  "heartbeat")

(def etlds (get-effective-tld-structure))

(defn construct-query [start-date end-date domain doi]
  (let [[subdomain, domain, tld] (get-main-domain (get-host domain) etlds)]
    {:subdomain subdomain
     :domain domain
     :tld tld
     :start-date start-date
     :end-date end-date
     :doi doi}))

(defn validate-query 
  "Validate query. Return empty vector on success or list of error strings" 
  [query]
  
  (let [got-domain (not (empty? (:domain query)))
        got-start (not (nil? (:start-date query)))
        got-end (not (nil? (:end-date query)))
        got-doi (not (empty? (:doi query)))
        
        validation-rules [
          (fn domain-or-doi [] (when (and
            (not got-domain)
            (not got-doi)) "DOI or domain or both must be supplied."))
          (fn start-end [] (when (or
            (not got-start)
            (not got-end)) "Both start and end date must be supplied."))]
        result (remove nil? ((apply juxt validation-rules)))]
    result))

(defn days 
  [params]
  (try
    (let [query (params :query-params)
          start-date-input (get query "start-date")
          end-date-input (get query "end-date")
          start-date (when start-date-input (parse date-formatter start-date-input))
          end-date (when end-date-input (parse date-formatter end-date-input))
          domain (get query "domain")
          doi (get query "doi")
          query (construct-query start-date end-date domain doi)
          validation (validate-query query)]
      (if (not (empty? validation))
        {:status 400 :headers {"Content-Type" "application/json"} :body (clojure.string/join "\n" validation)}
        {:status 200 :headers {"Content-Type" "application/json"} :body {
          :input {
            :start-date (unparse date-formatter start-date)
            :end-date (unparse date-formatter end-date)
            :doi doi
            :domain domain
            }
          :query query
          :result (storage/query-days query)}}))
    (catch IllegalArgumentException ex {:status 400 :headers {"Content-Type" "application/json"} :body (str "Date: " (.getMessage ex))})))

(defroutes app-routes
  (GET "/" [] (redirect "/index.html"))
  (GET "/heartbeat" [] heartbeat)
  (GET "/days" {params :params} (wrap-json-response days))
  (route/resources "/")  )

(def app
  (handler/site app-routes))
