(ns witan.models.dem.ccm.core.projection-loop-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.core.projection-loop :refer :all]
            [clojure.test :as t]
            [clojure.string :as str]
            [clojure.core.matrix.dataset :as ds]
            [clojure.core.matrix :as m]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

;; Load testing data
;; NEED TO GET THE RIGHT INPUT -> mye.est
(def data-inputs (ld/load-datasets
                  {:population
                   "resources/test_data/bristol_hist_popn_est.csv"
                   :births 
                   "resources/test_data/handmade_outputs/bristol_fertility_module_handmade_output.csv"
                   :deaths 
                   "resources/test_data/handmade_outputs/bristol_mortality_module_handmade_output.csv"
                   :net-migration 
                   "resources/test_data/handmade/outputs/bristol_migration_module_handmade_output.csv"}))

(def params {:first-proj-year 2014
             :last-proj-year 2017})

;; Useful fn:
(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

;; Tests:
(deftest select-starting-popn-test
  (testing "The starting popn is returned"
    (let [get-start-popn (select-starting-popn data-inputs)]
      (is (same-coll? [:gss-code :sex :age :year :popn]
                      (ds/column-names (:population get-start-popn))))
      (is (= 2015 (get-last-yr-from-popn (:population get-start-popn))))
      ;; Total number of 15 yrs old in 2015:
      (is (= 4386 (apply + (i/$ :popn
                                (i/query-dataset (:population get-start-popn)
                                                 {:year 2015 :age 15})))))
      ;; Total number of women aged between 40 and 43 in 2015:
      (is (= 5394 (apply + (i/$ :popn
                                (i/query-dataset (:population get-start-popn)
                                                 {:sex "F" :age {:$gt 40 :lt 43}
                                                  :year 2015}))))))))

(deftest age-on-test
  (testing "Popn is aged on correctly, 90 grouping is handled correctly."
    (let [starting-popn (:population (select-starting-popn data-inputs))
          aged-popn (:population (age-on (select-starting-popn data-inputs)))
          sum-popn (fn [popn year age]
                     (apply + (i/$ :popn (i/query-dataset popn
                                                          {:year year :age age}))))]
      (is (= (sum-popn starting-popn 2015 15)
             (sum-popn aged-popn 2015 16)))
      (is (= (+ (sum-popn starting-popn 2015 89) (sum-popn starting-popn 2015 90))
             (sum-popn aged-popn 2015 90)))
      (is (= 2 (first (:shape (i/query-dataset aged-popn {:year 2015 :age 90}))))))))
