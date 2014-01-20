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
                 [lein-ring "0.8.10"]
                 [javax.servlet/servlet-api "2.5"]
                 [org.clojure/clojure "1.5.1"]
                 [compojure "1.1.6"]
                 [ring/ring-json "0.2.0"]
                 ]
  :plugins [[lein-ring "0.8.10"]]
  :ring {:handler ology.handler/app}
  :main ^:skip-aot ology.core
  :target-path "target/%s"
  :jvm-opts ["-Xmx5g" "-server"]
  :profiles {:uberjar {:aot :all}}
  )
