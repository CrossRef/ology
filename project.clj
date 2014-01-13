(defproject ology "0.1.0-SNAPSHOT"
  :description "A tool for getting info out of CrossRef resolution logs."
  :url "http://github.com/CrossRef/ology"
  :dependencies [
                 [org.clojure/clojure "1.5.1"]
                 [com.novemberain/monger "1.5.0"]
                 [clj-http "0.7.8"]
                 [clj-time "0.6.0"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/tools.logging "0.2.6"]
                 [environ "0.4.0"]
                 ]
  :main ^:skip-aot ology.core
  :target-path "target/%s"
  :jvm-opts ["-Xmx3g" "-server"]
  :profiles {:uberjar {:aot :all}})
