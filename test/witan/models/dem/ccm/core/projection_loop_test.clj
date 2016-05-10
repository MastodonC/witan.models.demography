(ns witan.models.dem.ccm.core.projection-loop-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.core.projection-loop :refer :all]
            [clojure.test :as t]
            [clojure.string :as str]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

;; Load testing data
;; NEED TO GET THE RIGHT INPUT -> mye.est
(def data-inputs (ld/load-datasets
                  {:popn
                   "resources/test_data/bristol_hist_popn_est.csv"}))

(def params {:first-proj-year 2014
             :last-proj-year 2041})

;; Useful fn:
(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

;; Tests:
(deftest ->starting-popn-test
  (testing "The starting popn is returned"
    (let [get-start-popn (->starting-popn data-inputs params)]
      (is (same-coll? [:starting-popn :popn] (keys get-start-popn)))
      (is (same-coll? [:gss-code :sex :age :year :popn]
                      (ds/column-names (:starting-popn get-start-popn))))
      (is (same-coll? [2013] (set (i/$ :year (:starting-popn
                                              (->starting-popn data-inputs params)))))))))

(deftest ccm-core-test
  (testing "The intermediary outputs are added to the global map"
    (let [exec-core (ccm-core data-inputs params)]
      (is (contains? exec-core :starting-popn)))))
