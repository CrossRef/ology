(ns ology.handler
  (:use compojure.core)
  (:require [ology.core :as core])
  (:require [clojure.string])
  (:require [ology.storage :as storage])
  (:require [compojure.handler :as handler]
            [compojure.route :as route])
  (:use ring.middleware.json)
  (:use ring.util.response)
  (:use [clj-time.format])
)

(def date-formatter (formatter "yyyy-MM-dd"))
(def page-size 10)

(defn heartbeat [request]
  "heartbeat")

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn construct-query [start-date end-date domain doi]
  (let [[subdomain, domain, tld] (core/get-main-domain (core/get-host domain) core/etlds)]
    {:subdomain subdomain
     :domain domain
     :tld tld
     :start-date start-date
     :end-date end-date
     :doi doi}))

(defn validate-days-query 
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

(defn days-full
  [params]
  (try
    (let [query (params :query-params)
          group-method (get query "group-method")
          start-date-input (get query "start-date")
          end-date-input (get query "end-date")
          start-date (when start-date-input (parse date-formatter start-date-input))
          end-date (when end-date-input (parse date-formatter end-date-input))
          domain (get query "domain")
          doi (get query "doi")
          the-query (construct-query start-date end-date domain doi)
          validation (validate-days-query the-query)]
      (if (not (empty? validation))
        {:status 400 :headers {"Content-Type" "application/json"} :body (clojure.string/join "\n" validation)}
        {:status 200 :headers {"Content-Type" "application/json"} :body {
          :input {
            :start-date (unparse date-formatter start-date)
            :end-date (unparse date-formatter end-date)
            :doi doi
            :domain domain
            }
          :query the-query
          :result (storage/query-days the-query group-method)}}))
    (catch IllegalArgumentException ex {:status 400 :headers {"Content-Type" "application/json"} :body (str "Date: " (.getMessage ex))})))

(defn to-csv
  "Turn vector of simple maps into CSV. Columns must be provided for column order as vector of pairs of [symbol header-name]."
  [lines columns]
  (let [
    line-values (map (fn [line] (apply vector (map #(str (get line %)) (map first columns)))) lines)
    emit-values (cons (map second columns) line-values)
    csv-lines (apply vector (map #(clojure.string/join "," %) emit-values))
    result (clojure.string/join "\n" csv-lines)]
    result))

(defn days-csv
  [params]
  (try
    (let [query (params :query-params)
          group-method (get query "group-method")
          start-date-input (get query "start-date")
          end-date-input (get query "end-date")
          start-date (when start-date-input (parse date-formatter start-date-input))
          end-date (when end-date-input (parse date-formatter end-date-input))
          domain (get query "domain")
          doi (get query "doi")
          the-query (construct-query start-date end-date domain doi)
          validation (validate-days-query the-query)]
      (if (not (empty? validation))
        {:status 400 :headers {"Content-Type" "text/plain"} :body (clojure.string/join "\n" validation)}
        {:status 200 :headers {"Content-Type" "text/plain"}
         :body (to-csv (:days (storage/query-days the-query group-method)) [[:date "Date"] [:count "Count"]])}))
    (catch IllegalArgumentException ex {:status 400 :headers {"Content-Type" "text/plain"} :body (str "Date: " (.getMessage ex))})))

(defn top-domains 
  [params]
  (try
    (let [query (params :query-params)
          subdomain-rollup (case (get query "subdomain-rollup") nil nil "" nil "all" :all "day" :day "month" :month "year" :year nil)
          ignore-tld (case (get query "ignore-tld") nil false "true" true "false" false true)
          start-date-input (get query "start-date")
          end-date-input (get query "end-date")
          ; page is 1-indexed on the API.
          page-number (dec (parse-int (or (get query "page") "1")))
          start-date (when start-date-input (parse date-formatter start-date-input))
          end-date (when end-date-input (parse date-formatter end-date-input))]
      (if (or (empty? start-date-input) (empty end-date-input))
        {:status 400 :headers {"Content-Type" "application/json"} :body "Supply start-date and end-date parameters."}
        {:status 200 :headers {"Content-Type" "application/json"} :body
          (storage/query-top-domains start-date end-date subdomain-rollup ignore-tld page-number page-size)
        }))
    (catch IllegalArgumentException ex {:status 400 :headers {"Content-Type" "application/json"} :body  (.getMessage ex)})))


(defroutes app-routes
  (GET "/" [] (redirect "/app/index.html"))
  (GET "/heartbeat" [] heartbeat)
  (GET "/days" {params :params} (wrap-json-response days-full))
  (GET "/days-csv" {params :params} days-csv)
  (GET "/top-domains" {params :params} (wrap-json-response top-domains))
  (route/resources "/")  )

(def app
  (handler/site app-routes))
