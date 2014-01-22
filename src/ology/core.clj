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

(def ^String line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")

(def log-date-formatter (format/formatter (time/default-time-zone) "EEE MMM dd HH:mm:ss zzz yyyy" "EEE MMM dd HH:mm:ss ZZZ yyyy"))

;; Helper functions.
(defn domain-parts
  "Split a domain into its parts. If the domain is malformed, an empty vector."
  [^String domain]
  (try
    (clojure.string/split domain #"\.")
  (catch Exception _ [])))

;; Reading the eTLD file.
(defn etld-entry?
  "Is this a valid line in the eTLD file?"
  [^String line]
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
   [^String url]
   (try 
     (when (> (count url) 3)
       ; (println url)
       (.getHost (new URL url)))
    (catch Exception _ (try 
     (when (> (count url) 3)
       ; (println url)
       (.getHost (new URL (str "http://" url))))
    (catch Exception _ nil)))))

(defn intern-or-nil [x] (when x (.intern ^String x)))

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
        [(intern-or-nil (apply str (interpose "." subdomains))) (intern-or-nil main-domain) (intern-or-nil (apply str (interpose "." etld-parts)))]))

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
  [^String inp]
  (if (= (first inp) (last inp) \")
    (subs inp 1 (dec (count inp))) inp))

(defn convert-special-uri
  "For special uris, convert them into an HTTP host proxy form."
  [^String uri]
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
  [^String line]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            ; match is [ip, ?, date, ?, ?, ?, doi, ?, referrer]
            (let [;^String ip (match 1)
                  ^String date-str (strip-quotes (match 3))
                  ^String doi (.intern ^String (match 7)) 
                  ^String referrer-url (convert-special-uri (strip-quotes (match 9)))
                  ; Half-way type hint makes a lot of difference!
                  ^org.joda.time.DateTime date-prelim (format/parse log-date-formatter date-str)
                  ^org.joda.time.DateTime the-date (.withTimeAtStartOfDay date-prelim)
                  domain-triple (get-main-domain (get-host referrer-url) etlds)
                  ]                
                  [the-date doi domain-triple]))))


(defn partition-many-by [f comp-f s]
  (let [sorted-s (sort-by first comp-f s)
        first-list (first (drop-while (complement seq) sorted-s))
        match-val (f (first first-list))
        remains (filter #(not (empty? %)) 
                        (map #(drop-while (fn [ss] (= match-val (f ss))) %) 
                             sorted-s))]
    (when match-val
      (cons
        (apply concat
          (map #(take-while (fn [ss] (= match-val (f ss))) %)
               sorted-s))
        (lazy-seq (partition-many-by f comp-f remains))))))


(defn count-per-key
  "Map a map of [key vectors] to [key count]"
  [inp]
  (let [counts (reduce-kv (fn [m k v] (assoc m k (count v))) {} inp)
        count-vector (into [] counts)]
    (sort #(compare (second %2) (second %1)) count-vector)))

; Earlier and later date, allowing for nils.
(defn later-date [^org.joda.time.DateTime a ^org.joda.time.DateTime b] (if (nil? a) b (if (time/after? a b) a b)))
(defn earlier-date [^org.joda.time.DateTime a ^org.joda.time.DateTime b] (if (nil? a) b (if (time/before? a b) a b)))

(defn -main
  "Accept list of log file paths"
  [& input-file-paths]
  (info "Verify" (count input-file-paths) "input files.")  
  (info "Files:" input-file-paths)  
  
  (doseq [input-file-path input-file-paths]
    (let [the-file (clojure.java.io/file input-file-path)
          log-file-reader (clojure.java.io/reader the-file)]
          (info "Verify that input file " input-file-path " exists.")
          (.close log-file-reader)))
  
  (info "Finished verifying input files.")
  
  (let [files (map clojure.java.io/file input-file-paths)
        readers (map clojure.java.io/reader files)
        parsed-line-sequences (map (fn [reader] (remove nil? (map parse-line (line-seq reader)))) readers)
        date-partitions (partition-many-by first (fn [a b] (compare (first a) (first b))) parsed-line-sequences)   
        ]
        (doseq [date-partition date-partitions]
          (info "Start processing date partition for DOI calculation. " (first date-partition))
          
          (let [; Date of the first line of this partition of entries which all have the same date.
                ^org.joda.time.DateTime the-date (first (first date-partition))
                doi-freqs (frequencies (map #(get % 1) date-partition))
            ]
            
            (info "Calculated frequencies for partition. " the-date)
            (storage/insert-doi-freqs doi-freqs the-date)
            (info "Inserted.")
            ))
        (doseq [reader readers] (.close reader)))
        
  (let [files (map clojure.java.io/file input-file-paths)
        readers (map clojure.java.io/reader files)
        parsed-line-sequences (map (fn [reader] (remove nil? (map parse-line (line-seq reader)))) readers)
        date-partitions (partition-many-by first (fn [a b] (compare (first a) (first b))) parsed-line-sequences)   
        ]
        (doseq [date-partition date-partitions]
          (info "Start processing date partition for Domain calculation. " (first date-partition))
          
          (let [; Date of the first line of this partition of entries which all have the same date.
                ^org.joda.time.DateTime the-date (first (first date-partition))
                domain-freqs (frequencies (map #(get % 2) date-partition))
            ]
            
            (info "Calculated frequencies for partition. " the-date)
            (storage/insert-domain-freqs domain-freqs the-date)
            (info "Inserted.")
            ))
        (doseq [reader readers] (.close reader)))

  (let [files (map clojure.java.io/file input-file-paths)
        readers (map clojure.java.io/reader files)
        parsed-line-sequences (map (fn [reader] (remove nil? (map parse-line (line-seq reader)))) readers)
        date-partitions (partition-many-by first (fn [a b] (compare (first a) (first b))) parsed-line-sequences)   
        ]
            
        (doseq [date-partition date-partitions]
          (info "Start processing date partition for Domain x DOI calculation. " (first date-partition))
          
          (let [; Date of the first line of this partition of entries which all have the same date.
                ^org.joda.time.DateTime the-date (first (first date-partition))
                domain-doi-freqs (frequencies (map rest date-partition))
            ]
            
            (info "Calculated frequencies for partition. " the-date)
            (storage/insert-domain-doi-freqs domain-doi-freqs the-date)
            (info "Inserted.")
            ))
        (doseq [reader readers] (.close reader))
        
        
        ))
        
  