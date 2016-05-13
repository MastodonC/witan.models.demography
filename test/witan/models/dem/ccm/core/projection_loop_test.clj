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
                   "resources/test_data/bristol_hist_popn_est.csv"}))

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
      (is (= 2015 (get-last-yr-from-popn (:population get-start-popn)))))))
