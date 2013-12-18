(ns ology.core-test
  (:require [clojure.test :refer :all]
            [ology.core :refer :all])
  
  (:require [clj-time.format :refer [parse]]
            [clj-time.core :refer [year month day hour minute second]]))

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
    (let [parsed (parse log-date-formatter "Sun Sep 01 12:05:40 EDT 2013")]
      (is (= (year parsed) 2013))  
      (is (= (month parsed) 9))  
      (is (= (day parsed) 1))  
      (is (= (hour parsed) 16))  
      (is (= (minute parsed) 05))  
      (is (= (second parsed) 40)))))