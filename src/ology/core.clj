(ns ology.core
    (:gen-class)
    (:require [clj-http.client :as client])
    (:require [clojure.edn :as edn])
    (:require [clojure.java.io :as io])
    (:require [clj-time.format :refer [parse formatter]])
    (:require [ology.storage :as storage])
    (:import (org.joda.time.DateTimeZone))
    (:import (org.joda.time.TimeZone))
    (:import (java.net URL))
    (:import (java.text.SimpleDateFormat))
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.core :as time])
    (:require [clj-time.coerce :as coerce])
    (:require [clj-time.format :as format])
    (:use [environ.core])    
    )

(def ^String line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")

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
  "Parse a line from the log, return vector of [date, doi, domain-triple]"
  [^String line]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            ; match is [ip, ?, date, ?, ?, ?, doi, ?, referrer]
            (let [;^String ip (match 1)
                  ^String date-str (strip-quotes (match 3))
                  ^String doi (match 7)
                  ^String referrer-url (convert-special-uri (strip-quotes (match 9)))
                  ; Half-way type hint makes a lot of difference!
                  ^org.joda.time.DateTime date-prelim (format/parse log-date-formatter date-str)
                  ^org.joda.time.DateTime the-date (time/date-time (time/year date-prelim) (time/month date-prelim) (time/day date-prelim))
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

(defn scatter-file
  "Take a filename, parse and bin entries by day in a file-per-day in dir-base."
  [input-file-path dir-base]
  (info "Preprocessing input file" input-file-path)
  (with-open [log-file-reader (clojure.java.io/reader (clojure.java.io/file input-file-path))]
    (let [parsed-lines (remove nil? (map parse-line (line-seq log-file-reader)))]
      (loop [lines parsed-lines
             current-output-file nil
             current-output-file-date nil
             ]
        (let [line (first lines)
              line-date (first line)
              line-doi-domain (rest line)
              the-rest (rest lines)]
          
        (if (= line-date current-output-file-date)
            ;; If we're in a run of the same date, write out.
            (do 
              (.write current-output-file (pr-str line-doi-domain))
              (.write current-output-file "\n")
              (if
                (not (empty? the-rest)) (recur the-rest current-output-file current-output-file-date)
                (.close current-output-file)
              )
            )
            
            ;; Otherwise close the old file (if there is one) and open a new one.
            (do
              (when current-output-file
                (info "Closed" current-output-file-date)
                (.close current-output-file))
              (let [filename (format/unparse scatter-file-date-formatter line-date)
                    new-output-file (clojure.java.io/writer (str dir-base "/" filename) :append true)]
                    (info "Opened" filename)
                    (.write new-output-file (pr-str line-doi-domain))
                    (.write new-output-file "\n")
                    (if 
                      (not (empty? the-rest))
                      (recur the-rest new-output-file line-date)
                      (.close current-output-file))))))))))

(defn gather-files
  "For the preprocessed files generated by scatter-file, calculate frequencies and insert into Mongo."
  [dir-base]
  (info "Calculating frequencies for date bins.")
  (let [file-paths (filter #(.isFile %) (file-seq (clojure.java.io/file dir-base)))]
    (doseq [the-file file-paths]
      (let [the-date (format/parse scatter-file-date-formatter (.getName the-file))]
        (info "Calculating for" the-date)
        (with-open [reader (clojure.java.io/reader the-file)]
          (let [lines (line-seq reader)
                parsed-lines (map clojure.edn/read-string lines)
                interned-parsed-lines (map (fn [[doi [subdomain domain tld]]] [(intern-or-nil doi) [(intern-or-nil subdomain) (intern-or-nil domain) (intern-or-nil tld)]]) parsed-lines)
                
                ; Each line is [doi [subdomain domain doi]], in the right format for frequencies.
                domain-doi-freqs (frequencies interned-parsed-lines) 
                doi-freqs (frequencies (map first interned-parsed-lines)) 
                domain-freqs (frequencies (map second interned-parsed-lines))
                ]
            (info "Insert domain freqs")
            (storage/insert-domain-doi-freqs domain-doi-freqs the-date)
            (info "Insert DOI freqs")
            (storage/insert-doi-freqs doi-freqs the-date)
            (info "Insert domain x DOI freqs")
            (storage/insert-domain-freqs domain-freqs the-date)
            (info "Done inserting for date " the-date)
            )))
      ; Delete the file if this ran successfully. 
      (info "Deleting" the-file)
      (delete-file the-file)

    )))

(defn -main
  "Accept list of log file paths"
  [temp-dir & input-file-paths]
  (info "Verify" (count input-file-paths) "input files.")  
  (info "Files:" input-file-paths)  
  
  ; First try opening each input file to check it exists.
  (doseq [input-file-path input-file-paths]
    (let [the-file (clojure.java.io/file input-file-path)
          log-file-reader (clojure.java.io/reader the-file)]
          (info "Verify that input file " input-file-path " exists.")
          (.close log-file-reader)))
  (info "Finished verifying input files.")
  
  ; Don't change the temp file if there are no input files (assume we want to try re-processing).
  (when (> (count input-file-paths) 0)
    ; Remove and create the temp directory.
    (when (.exists (clojure.java.io/file temp-dir))
        (delete-file-recursively temp-dir))
        (.mkdirs (clojure.java.io/file temp-dir))
        
        ; For each input file split into bins.
        (doseq [input-file-path input-file-paths]
          (scatter-file input-file-path temp-dir)))
  
  ; When the bins are filled, calculate and insert frequencies.
  (gather-files temp-dir)
)