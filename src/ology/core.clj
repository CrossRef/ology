(ns ology.core
    (:gen-class)
    (:require [clj-http.client :as client])
    (:require [clj-time.format :refer [parse formatter]])
    (:require [ology.storage :refer [insert-log-entry insert-log-entries drop-log-index ensure-log-index]])
)

(import java.net.URL)

(def line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")

(def log-date-formatter (formatter "EEE MMM dd HH:mm:ss zzz yyyy"))

;; External APIs.
; This is designed to work against the ajutor DOI RA service which returns extra headers for speed.
(def doi-registration-authority-service-url "http://habanero:8000/ra/")


(defn validate-doi
  "Check if a CrossRef DOI is validate-doi. Return true or false. Throws java.net.SocketException"
  [doi]
  (let
    [response (client/get (str doi-registration-authority-service-url doi) {:accept :json :as :json})]
    (= (-> response :headers "DOI-RA") "CrossRef")))

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
  ; (prn "CSI" uri)
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
  "Parse a line from the log, return vector of [ip, date, doi, referrer URL]"
  [line]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            (let [ip (match 1)
                  date (strip-quotes (match 3))
                  doi (match 7)
                  referrer-url (convert-special-uri  (strip-quotes (match 9)))
                  ]
                [ip date doi referrer-url]))))

(defn db-insert-format
  "Take a vector of [ip date doi referrer-url ] and return a format for insertion into the mongo db."
  [[ip date doi referrer-url] etlds]
  (let [main-domain (get-main-domain (get-host referrer-url) etlds)]
  {
   :ip ip
   :date (parse log-date-formatter date)
   :doi doi
   :referrer referrer-url
   :subdomains (main-domain 0)
   :domain (main-domain 1)
   :tld (main-domain 2)}))

(defn url-referrals
  "From a sequence of log lines return a sequence of [referral URL, DOI]"
  [lines]
  (filter
    ; Exclude those without referral URLs.
    (fn [[url _]] (not (empty? url)))
    (map
      #(vector (% 3) (% 2))
      (remove nil? (map parse-line lines)))))

(defn subdomain-referrals
  "From a sequence of [referral URL, DOI] return a sequence of [[subdomains, domain], doi]"
  [referrals etlds]
  (map (fn [[url doi]] [(get-main-domain (get-host url) etlds) doi])
       referrals))

(defn count-per-key
  "Map a map of [key vectors] to [key count]"
  [inp]
  (let [counts (reduce-kv (fn [m k v] (assoc m k (count v))) {} inp)
        count-vector (into [] counts)]
    (sort #(compare (second %2) (second %1)) count-vector)))


(defn -main [input-file-path]
  (with-open [log-file-reader (clojure.java.io/reader input-file-path)]
    (let [log-file-seq (line-seq log-file-reader)
          referrals (url-referrals log-file-seq)
          etlds (get-effective-tld-structure)
          
          ; Filter out lines that don't parse.
          parsed-lines (remove nil? (map parse-line log-file-seq))
          db-insert-format (map #(db-insert-format %1 etlds) parsed-lines)
          ]
      (prn "Load" input-file-path)
      (ensure-log-index)
      ; (prn "Drop index")
      ; (drop-log-index)
      ; (doseq [item db-insert-format] (insert-log-entries item))
      
      (doseq [batch (partition 10 db-insert-format)] (prn batch) (insert-log-entries batch) (prn "**"))
    
    
      ; (prn "Reindex")
      ; (ensure-log-index)
      ; (prn "Done")
    )
  )
)
