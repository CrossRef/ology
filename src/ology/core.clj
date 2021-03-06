(ns ology.core
    (:gen-class)
    (:require [clj-http.client :as client])
    (:require [clojure.edn :as edn])
    (:require [clojure.string :as string])
    (:require [clojure.java.io :as io])
    (:require [clj-time.format :refer [parse formatter]])
    (:require [ology.storage :as storage]
              [ology.config :refer [config]])
    (:import (org.joda.time.DateTimeZone))
    (:import (org.joda.time.TimeZone))
    (:import (java.net URL))
    (:import (java.text.SimpleDateFormat))
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.core :as time])
    (:require [clj-time.coerce :as coerce])
    (:require [clj-time.format :as format])
    (:use [environ.core]))

(def ^String line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")

; The URI filter for experimental tweaking.
; If supplied, filter only to this domain.
(def special-url-filter 
  (or (env :url-filter) nil))

(defn bin-for-value
  "Return the bin number for a given string (doi or domain)"
  [^String value bin-size]
  ; It's just a checksum.
  (mod (reduce + (map int value)) bin-size))

(if special-url-filter (info "URL Filter:" special-url-filter ))

; Formatters for reading log file.
(def log-date-formatter (format/formatter (time/default-time-zone) "EEE MMM dd HH:mm:ss zzz yyyy" "EEE MMM dd HH:mm:ss ZZZ yyyy"))

; Format for the file names of 'scatter' files.
(def scatter-file-date-formatter (format/formatter "yyyy-MM-dd"))

(defn delete-file
  "Delete file f. Raise an exception if it fails unless silently is true."
  [f & [silently]]
  (or (.delete (clojure.java.io/file f))
      silently
      (throw (java.io.IOException. (str "Couldn't delete " f)))))

(defn delete-file-recursively
  "Delete file f. If it's a directory, recursively delete all its contents.
Raise an exception if any deletion fails unless silently is true."
  [f & [silently]]
  (let [f (clojure.java.io/file f)]
    (if (.isDirectory f)
      (doseq [child (.listFiles f)]
        (delete-file-recursively child silently)))
    (delete-file f silently)))

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

; (defn intern-or-nil [x] (when x (.intern ^String x)))
(defn intern-or-nil [x] x)

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
  "Parse a line from the log, return vector of [date, doi, domain-triple]. Truncate date depending on period-type, one of [:day :month]."
  [^String line period-type]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            ; match is [ip, ?, date, ?, ?, ?, doi, ?, referrer]
            (let [;^String ip (match 1)
                  ^String date-str (strip-quotes (match 3))
                  ^String doi (match 7)
                  ^String referrer-url (convert-special-uri (strip-quotes (match 9)))
                  ^org.joda.time.DateTime date-prelim (format/parse log-date-formatter date-str)
                  
                  ; Perform appropraite truncation.
                  ^org.joda.time.DateTime the-date (if (= period-type :day)
                                                     (time/date-time (time/year date-prelim) (time/month date-prelim) (time/day date-prelim))
                                                     (time/date-time (time/year date-prelim) (time/month date-prelim)))
                  domain-triple (get-main-domain (get-host referrer-url) etlds)
                  ]
              
                  (when (or (nil? special-url-filter) (.contains referrer-url special-url-filter))
                    [the-date doi domain-triple])))))


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

(def types [:doi :domain :doi-domain])
(def doi-type-index 0)
(def domain-type-index 1)
(def doi-domain-type-index 2)

(defn open-bin-files
  "Return vector of vectors binned files for date, indexed by type [doi, domain-doi, domain] then bin number"
  [the-date num-bins dir-base]
  
  (let [bin-numbers (range num-bins)
        bin-file-names (map #(str (format/unparse scatter-file-date-formatter the-date) "~" %) bin-numbers)
        new-output-files (map (fn [type] (map (fn [bin-name] (clojure.java.io/writer (str dir-base "/" bin-name "~" (name type)) :append true)) bin-file-names)) types)]
    (apply vector new-output-files)))

(defn close-bin-files
  [bin-files]
  (doseq [file (flatten bin-files)] (.close file)))

(defn get-output-file
  "From a structure of current output files (produced from open-bin-files) return the correct one for the given line and type."
  [output-files line type-index num-bins]
  (let [for-type (nth output-files type-index)
        bin-number (bin-for-value (apply str line) num-bins)]
       (nth for-type bin-number)))

(defn scatter-file
  "Take a filename, parse and bin entries by day in a file-per-day or file-per-month (depending on period-type) in dir-base."
  [input-file-path dir-base period-type num-bins]
  (info "Preprocessing input file" input-file-path)
  (with-open [log-file-reader (clojure.java.io/reader (clojure.java.io/file input-file-path))]
      (loop [lines (remove nil? (map #(parse-line % period-type) (line-seq log-file-reader)))
             current-output-files nil
             current-output-file-date nil
             ]
        (let [line (first lines)
              line-date (first line)
              line-doi-domain (rest line)
              the-rest (rest lines)]
        (if (= line-date current-output-file-date)
            ;; If we're in a run of the same date, write out.
            (do 
                    
              ; DOI
              (.write (get-output-file current-output-files (first line-doi-domain) doi-type-index num-bins) (str line-doi-domain \newline))
              
              ; Domain
              (.write (get-output-file current-output-files (second line-doi-domain) domain-type-index num-bins) (str line-doi-domain \newline))

              ; Both
              (.write (get-output-file current-output-files line-doi-domain doi-domain-type-index num-bins) (str line-doi-domain \newline))
            
              (if
                (not (empty? the-rest)) (recur the-rest current-output-files current-output-file-date)
                (close-bin-files current-output-files)
              )
            )
            
            ;; Otherwise close the old file (if there is one) and open a new one.
            (do
              (when current-output-files
                (info "Closed" current-output-file-date)
                (close-bin-files current-output-files))
              (let [new-output-files (open-bin-files line-date num-bins dir-base)]
                    
                    ; DOI
                    (.write (get-output-file new-output-files (first line-doi-domain) doi-type-index num-bins) (str line-doi-domain \newline))
                    
                    ; Domain
                    (.write (get-output-file new-output-files (second line-doi-domain) domain-type-index num-bins) (str line-doi-domain \newline))

                    ; Both
                    (.write (get-output-file new-output-files line-doi-domain doi-domain-type-index num-bins) (str line-doi-domain \newline))

                    (if 
                      (not (empty? the-rest))
                      (recur the-rest new-output-files line-date)
                      (close-bin-files new-output-files)))))))))

(defn gather-files
  "For the preprocessed files generated by scatter-file, calculate frequencies and insert into Mongo."
  [dir-base period-type bin-size]
  (info "Calculating frequencies for date bins.")
  (let [file-paths (filter #(.isFile %) (file-seq (clojure.java.io/file dir-base)))]
    (doseq [the-file file-paths]
      (let [filename (.getName the-file)
            [date-part bin-part type-part] (string/split filename #"~")
            the-date (format/parse scatter-file-date-formatter date-part)]
        (info "Calculating for" the-date "bin" bin-part "type" type-part)
        
        
        (when (= type-part "doi")
          (with-open [reader (clojure.java.io/reader the-file)]
            (let [doi-freqs (->> reader
            line-seq
            (map clojure.edn/read-string)
            ; Pick only the DOI.
            (map first)
            frequencies)]
              
            (info "Insert DOI freqs")
            (storage/insert-doi-freqs doi-freqs the-date period-type)
            (info "Done inserting DOI freqs for bin"))))

        (when (= type-part "domain")
          (with-open [reader (clojure.java.io/reader the-file)]
            (let [domain-freqs (->> reader
            line-seq
            (map clojure.edn/read-string)
            ; Pick only the DOI.
            (map second)
            frequencies)]
              
            (info "Insert Domain freqs")
            (storage/insert-domain-freqs domain-freqs the-date period-type)
            (info "Done inserting Domain freqs for bin"))))

        (when (= type-part "doi-domain")
          (with-open [reader (clojure.java.io/reader the-file)]
            (let [doi-domain-freqs (->> reader
            line-seq
            (map clojure.edn/read-string)
            ; Take both
            frequencies)]
              
            (info "Insert DOI Domain freqs")
            (storage/insert-domain-doi-freqs doi-domain-freqs the-date period-type)
            (info "Done inserting DOI Domain freqs for bin"))))
        
        (info "Deleting" the-file)
        (delete-file the-file)))))



(defn main-ingest
  "Ingest log files."
   [temp-dir input-file-paths]
   (info "Command: ingest")
   
   (info "Verify" (count input-file-paths) "input files.")  
  (info "Files:" input-file-paths)  
  
  ; First try opening each input file to check it exists.
  (doseq [input-file-path input-file-paths]
    (let [the-file (clojure.java.io/file input-file-path)
          log-file-reader (clojure.java.io/reader the-file)]
          (info "Verify that input file " input-file-path " exists.")
          (.close log-file-reader)))
  (info "Finished verifying input files.")
  
  (let [temp-dir-day (str temp-dir "/day")
        temp-dir-month (str temp-dir "/month")]

    ; Don't change the temp file if there are no input files (assume we want to try re-processing).
    (when (> (count input-file-paths) 0)

      ; Remove and create the temp directories.
      (when (.exists (clojure.java.io/file temp-dir-day))
          (info "Removing old files" temp-dir-day)
          (delete-file-recursively temp-dir-day))
          (.mkdirs (clojure.java.io/file temp-dir-day))
          
      (when (.exists (clojure.java.io/file temp-dir-month))
          (info "Removing old files" temp-dir-month)
          (delete-file-recursively temp-dir-month))
          (.mkdirs (clojure.java.io/file temp-dir-month))
          
          ; For each input file split into bins.
          (doseq [input-file-path input-file-paths]
            (scatter-file input-file-path temp-dir-day :day 1)
            (scatter-file input-file-path temp-dir-month :month 30)))
    
    ; When the bins are filled, calculate and insert frequencies.
    ; Single bin for per-day (known to fit in RAM comfortably), more bins per-month.
    (gather-files temp-dir-day :day 1)
    (gather-files temp-dir-month :month 30)))

