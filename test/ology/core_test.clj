(ns ology.core-test
  (:require [clojure.test :refer :all]
            [ology.core :refer :all])
  
  (:require [clj-time.format :refer [parse]]
            [clj-time.core :as time]
            [clj-time.coerce :as coerce])
  )

(deftest e-tlds
  (testing "Verify that get-main-domain does what it's meant to."
    (let [etlds (get-effective-tld-structure)
          expected [["test.com" ["" "test" "com"]]
                    ["www.test.com" ["www" "test" "com"]]
                    ["www.xyz.test.com" ["www.xyz" "test" "com"]]
                    ["www.xyz.any.number.of.subdomains.test.com" ["www.xyz.any.number.of.subdomains" "test" "com"]]
                    ["www.test.co.uk" ["www" "test" "co.uk"]]
                    ["test.co.uk" ["" "test" "co.uk"]]
                    ["localhost" ["" "localhost" ""]]
                    ["my.not-a-real-domain" ["my" "not-a-real-domain" ""]]
                    ]]
      (doall (map
               (fn [[input expected-output]] (is (= (get-main-domain input etlds) expected-output))) 
               expected)))))


(deftest date-formatter-test
  (testing "Date format works"
    (let [parsed (coerce/from-date (. log-date-formatter parse "Sun Sep 01 12:05:40 EDT 2013"))]
      (is (= (time/year parsed) 2013))  
      (is (= (time/month parsed) 9))  
      (is (= (time/day parsed) 1))  
      (is (= (time/hour parsed) 16))  
      (is (= (time/minute parsed) 5))  
      (is (= (time/second parsed) 40))
      )
    (let [parsed (coerce/from-date (. log-date-formatter parse "Tue Apr 16 17:09:33 UTC 2013"))]
      (is (= (time/year parsed) 2013))  
      (is (= (time/month parsed) 4))  
      (is (= (time/day parsed) 16))  
      (is (= (time/hour parsed) 17))  
      (is (= (time/minute parsed) 9))  
      (is (= (time/second parsed) 33)))))

