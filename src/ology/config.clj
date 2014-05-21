(ns ology.config
    (:require [clojure.tools.reader.edn :as edn])
    (:require [clojure.tools.logging :refer [info]]))
  
(def config-files ["config.default.edn" "config.edn"])

(def first-extant-file (first (filter #(.exists (clojure.java.io/as-file %)) config-files)))

(def config
    (let [file-to-use first-extant-file
          config-file (slurp file-to-use)
          the-config (edn/read-string config-file)]
              (info "Using config file" first-extant-file)
              the-config))