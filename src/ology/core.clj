(ns ology.core
    (:gen-class)
    (:require [clj-http.client :as client])
    (:require [clj-time.format :refer [parse formatter]])
    (:require [ology.storage :as storage])
    (:import (org.joda.time.DateTimeZone))
    (:import (org.joda.time.TimeZone))
    (:import (java.net URL))
    (:import (java.text.SimpleDateFormat))
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.core :as time])
    (:require [clj-time.format :as format])
    (:use [environ.core])    
    )

(def line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")

(def log-date-formatter (format/formatter "EEE MMM dd HH:mm:ss ZZZ yyyy"))

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

(def etlds (get-effective-tld-structure))

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
  "Parse a line from the log, return vector of [date, doi, domain-triple]"
  [line]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            ; match is [ip, ?, date, ?, ?, ?, doi, ?, referrer]
            (let [ip (match 1)
                  date-str (strip-quotes (match 3))
                  doi (match 7)
                  referrer-url (convert-special-uri  (strip-quotes (match 9)))
                  the-date (.withTimeAtStartOfDay (format/parse log-date-formatter date-str))
                  domain-triple (get-main-domain (get-host referrer-url) etlds)
                  ]
                
                  [the-date doi domain-triple]))))

(defn count-per-key
  "Map a map of [key vectors] to [key count]"
  [inp]
  (let [counts (reduce-kv (fn [m k v] (assoc m k (count v))) {} inp)
        count-vector (into [] counts)]
    (sort #(compare (second %2) (second %1)) count-vector)))

; Earlier and later date, allowing for nils.
(defn later-date [a b] (if (nil? a) b (if (time/after? a b) a b)))
(defn earlier-date [a b] (if (nil? a) b (if (time/before? a b) a b)))

(defn -main
  "Accept list of log file paths"
  [& input-file-paths]
  (let [etlds (get-effective-tld-structure)]    
    (doseq [input-file-path input-file-paths]
      (info "Inserting from " input-file-path)
      (with-open [log-file-reader (clojure.java.io/reader input-file-path)]
        (let [log-file-seq (line-seq log-file-reader)

              ; Filter out lines that don't parse.
              parsed-lines (remove nil? (map parse-line log-file-seq))

              ; Partition into sequences of date.   
              ; The first element of the parsed result is the date. The other two are the thing we want to keep. 
              date-partitions (partition-by first parsed-lines)
              ]
          
          (doseq [date-partition date-partitions]
            (info "Process date partition. ")
            
            (let [; Date of the first line of this partition of entries which all have the same date.
                  the-date (first (first date-partition))
                  freqs (frequencies (map rest date-partition))
              ]
              (info "Calculated frequencies for partition. " the-date)
              (storage/insert-freqs freqs the-date))))))))
          
    