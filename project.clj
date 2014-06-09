(defproject ology "0.1.0-SNAPSHOT"
  :description "A tool for getting info out of CrossRef resolution logs."
  :url "http://github.com/CrossRef/ology"
  :dependencies [
                 [com.novemberain/monger "1.5.0"]
                 [clj-http "0.7.8"]
                 [clj-time "0.7.0"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/tools.logging "0.2.6"]
                 [environ "0.4.0"]
                 [lein-ring "0.8.10"]
                 [javax.servlet/servlet-api "2.5"]
                 [org.clojure/clojure "1.6.0"]
                 [compojure "1.1.6"]
                 [ring/ring-json "0.2.0"]
                 [http-kit "2.1.10"]
                 [monetdb/monetdb-jdbc "2.11"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [org.clojure/java.jdbc "0.3.3"]]
  :plugins [[lein-ring "0.8.10"] [lein-daemon "0.5.4"]]
  :ring {:handler ology.handler/app}
  :main ^:skip-aot ology.main
  :target-path "target/%s"
  :jvm-opts ["-Xmx5g" "-server"]
  :profiles {:uberjar {:aot :all}}
  :daemon {:ology {:ns ology.main :pidfile "ology.pid"}})
