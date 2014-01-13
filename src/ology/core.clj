(ns ology.core
    (:gen-class)
    (:require [clj-http.client :as client])
    (:require [clj-time.format :refer [parse formatter]])
    (:require [ology.storage :as storage])
    (:import (java.security MessageDigest))
    (:import (java.net URLEncoder))
    (:import (org.joda.time.DateTimeZone))
    (:import (org.joda.time.TimeZone))
    (:import (java.text.SimpleDateFormat))
    (:use [clojure.tools.logging :only (info error)])
    (:use [environ.core])
    )


; The URI filter for experimental tweaking.
; If supplied, filter only to this domain.
(def special-url-filter 
  (or (env :url-filter) "")
  )

(prn "SUF" special-url-filter)

(import java.net.URL)

(def line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")

(def log-date-formatter (new java.text.SimpleDateFormat "EEE MMM dd HH:mm:ss zzz yyyy"))

(def batch-size 10000)

; Keep a list of known DOIs for this run. Used for filtering in the aggregation later.
(def known-dois (atom #{}))

(defn checksum
  "Generate a checksum for the given string"
  ; TODO removing the duplicate checking renders this unused. Remove.
  [token]
  (let [hash-bytes
         (doto (java.security.MessageDigest/getInstance "SHA1")
               (.reset)
               (.update (.getBytes token)))]
       (.toString
         (new java.math.BigInteger 1 (.digest hash-bytes)) ; Positive and the size of the number
         20)))

;; External APIs.
; This is designed to work against the ajutor DOI RA service which returns extra headers for speed.
(def doi-registration-authority-service-url "http://habanero:8000/ra/")

(defn validate-doi
  "Check if a DOI is valid and registered with CrossRef. Return true or false or :error if the upstream server isn't availble."
  [doi]
  (try 
  (let [response (client/get (str doi-registration-authority-service-url (URLEncoder/encode doi)) {:retry-handler (fn [ex try-count http-context] (< try-count 4))})]
    (= (get (:headers response) "doi-ra") "CrossRef"))
  (catch Exception _ :error)))

(defn validate-doi-not
  "Pretend to validate the DOI but don't (it's too slow)."
  [doi]
  :error)



;; Helper functions.

(defn domain-parts
  "Split a domain into its parts. If the domain is malformed, an empty vector."
  [domain]
  (try
    (clojure.string/split domain #"\.")
  (catch Exception _ [])))


;; Reading the eTLD file.

(defn etld-entry?
  "Is this a valid line in the eTLD file?"
  [line]
  (or (empty? line) (.startsWith line "//")))


(defn get-effective-tld-structure 
  "Load the set of effective TLDs into a trie."
  []
  (with-open [reader (clojure.java.io/reader (clojure.java.io/resource "effective_tld_names.dat"))]
    (let [lines (line-seq reader)
          components (map domain-parts (remove etld-entry? lines))
          tree (reduce #(assoc-in %1 (reverse %2) {}) {} components)]
      (do tree))))


;; Extracting domain info.

(defn get-host 
  "Extract the host from a URL string. If the scheme is missing, try adding http."
   [url]
   (try 
     (when (> (count url) 3)
       ; (println url)
       (.getHost (new URL url)))
    (catch Exception _ (try 
     (when (> (count url) 3)
       ; (println url)
       (.getHost (new URL (str "http://" url))))
    (catch Exception _ nil)))))


(defn get-main-domain
  "Extract the main (effective top-level domain, 'main domain' and subdomains) from a domain name. 'www.xxx.test.com' -> ['www.xxx' 'test' 'com'] . Return reversed vector of components."
  [domain etld-structure]

  ; Recurse to find the prefix that doesn't comprise a recognised eTLD.
  (defn find-tld-suffix [input-parts tree-parts]
    (let [input-head (first input-parts)
          input-tail (rest input-parts)
          
          ; Find children that match.
          tree-children (get tree-parts input-head)
          
          ; Or children that are wildcards.
          tree-children-wildcard (get tree-parts "*")]
      
        ; First try an exact match.
        (if (not (nil? tree-children))
          ; If found, recurse.
          (find-tld-suffix input-tail tree-children)
          
          ; If there isn't an exact match, see if wildcards are allowed.
          (if (not (nil? tree-children-wildcard))
            (find-tld-suffix input-tail tree-children-wildcard)
            input-parts))))
  
  (let [parts (domain-parts domain)
        reverse-parts (reverse parts)
        parts-length (count parts)
        non-etld-parts (find-tld-suffix reverse-parts etld-structure)
        etld-parts (drop (count non-etld-parts) parts)
        main-domain (first non-etld-parts)
        subdomains (reverse (rest non-etld-parts))]
        [(apply str (interpose "." subdomains)) main-domain (apply str (interpose "." etld-parts))]))

;; Hoofing data.


(defn append-in
  "For a sequence of [[x 1] [x 2] [y 3] [y 3]] return a map of {x [1 2] y [3 3]}"
  [m key val]
  (assoc m key (cons val (m key []))))


(defn domainFrequency
  "From a Seq of URL strings return a hash of the domains and their frequencies."
   [urls]
   (frequencies (map get-host urls)))

;; Processing log entries

(defn strip-quotes 
  "Strip quotes from the start and end of the line if they exist."
  [inp]
  (if (= (first inp) (last inp) \")
    (subs inp 1 (dec (count inp))) inp))

(defn convert-special-uri
  "For special uris, convert them into an HTTP host proxy form."
  [uri]
  (cond 
    
    ; Prefixes.
    (. uri startsWith "app:/ReadCube.swf") "http://readcube.special"
    (. uri startsWith "http://t.co") "http://api.twitter.com"
    
    ; Extra weird things.
    
    ; muc=1;Domain=t.co;Expires=Fri, 14-Aug-2015 16:48:09 GMT
    (. uri contains "Domain=t.co") "http://api.twitter.com"
      
    ; When there's no referrer, record that as a special value.
    (= uri "") "http://no-referrer"
    
    :else uri)
)

(defn parse-line 
  "Parse a line from the log, return vector of [ip, date, doi, referrer URL, original]"
  [line]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            (let [ip (match 1)
                  date (strip-quotes (match 3))
                  doi (match 7)
                  referrer-url (convert-special-uri  (strip-quotes (match 9)))]
                
                ; We have a DOI. Add this to the list of known DOIs.
                ; Check at this point that the domain matches. An empty string will return true.                
                (when (.contains referrer-url special-url-filter)
                  (swap! known-dois conj doi)
                  [ip date doi referrer-url line])))))

(defn db-insert-format
  "Take a vector of [ip date doi referrer-url ] and return a format for insertion into the mongo db.
  Mongo doesn't allow indexing on more than 1k, and some of the refferer logs are longer than that. So we hash and index on that.
  If there was an error deciding whether or not the DOI was valid, store that for later.
  "
  [[ip the-date doi referrer-url original] is-valid etlds]
  
  (let [main-domain (get-main-domain (get-host referrer-url) etlds)] {
   storage/ip-address ip
   storage/date-field (. log-date-formatter parse the-date)
   storage/doi doi
   storage/referrer referrer-url
   storage/subdomain (main-domain 0)
   storage/domain (main-domain 1)
   storage/tld (main-domain 2)
   ; storage/hashed (checksum original)
   storage/followup-ra (= is-valid :error)
   }))


(defn count-per-key
  "Map a map of [key vectors] to [key count]"
  [inp]
  (let [counts (reduce-kv (fn [m k v] (assoc m k (count v))) {} inp)
        count-vector (into [] counts)]
    (sort #(compare (second %2) (second %1)) count-vector)))

(defn -main
  "Accept filename as first argument, then arguments, such as [drop-index-first, build-index-first, build-index-after]"
  [& input-file-paths]
  (let [etlds (get-effective-tld-structure)]
    (storage/create-intermediate-collection)
    
    (doseq [input-file-path input-file-paths]
      (info "Inserting from " input-file-path)
      (with-open [log-file-reader (clojure.java.io/reader input-file-path)]
        (let [log-file-seq (line-seq log-file-reader)

              ; Filter out lines that don't parse.
              parsed-lines (remove nil? (map parse-line log-file-seq))
              
              ; Zip with CrossRef DOI validation check, return tuples of [parsed, valid]
              with-validation (map (fn [line] [line (validate-doi-not (get line 2))]) parsed-lines)
              
              ; Remove those that are known not to be valid. Let through errors.
              crossref-lines (filter #(not= (get % 1) false) with-validation)
              
              ; Transform into the right format for insertion into Mongo.
              db-insert-format-lines (map (fn([[parsed-line is-valid]]
                                              (db-insert-format parsed-line is-valid etlds))) with-validation)
              ]
          (info "Insert into intermediate collection")
          (doseq [batch (partition batch-size batch-size nil db-insert-format-lines)]
            (info (str "Insert Batch length " (count batch) " starting " (first batch)))
            (storage/insert-log-entries batch)))
        (info (str "Done inserting from " input-file-path))
        
        ))

    (info (str "Found " (count @known-dois) " DOIs"))
    
    (info "Getting inserted date range")
    (let [[start-date end-date] (storage/get-date-range)]
      (info (str "Clearing aggregates table between " start-date " and " end-date))
      (storage/clear-aggregates-for-date-range start-date end-date)

      (info "Running aggregation")
      (storage/update-aggregates-group-partitioned start-date end-date @known-dois)
      
      )
          
    (info "Done")))