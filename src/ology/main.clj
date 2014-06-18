(ns ology.main
    (:require [ology.storage :as storage]
              [ology.core :as core]
              [ology.handler :as handler]
              [ology.config :refer [config]])
    (:use [clojure.tools.logging :only (info error)])
        (:require [org.httpkit.server :as hs]))

(defn main-server
  "Run server."
   []
   (info "Command: server on" (:port config))
   (hs/run-server handler/app {:port (:port config)}))

(defn -main
  "Main method run by lein run and lein daemon."
  [command & [temp-dir & input-file-paths]]
    (cond
    (= command "server") (main-server)
    (= command "ingest") (core/main-ingest :mongo temp-dir input-file-paths)
    (= command "ingest-monetdb") (core/main-ingest :monetdb input-file-paths)
    (= command "aggregate-monetdb") (core/main-aggregate-monetdb)))