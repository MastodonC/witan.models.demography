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
                  {:historic-popn-estimates "resources/test_data/bristol_mye_coc.csv"}))

(def params {:first-proj-year 2014
             :last-proj-year 2040})

;; Useful fn:
(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

;; Tests:
(deftest ->starting-popn-test
  (testing "The starting popn is returned"
    (let [get-start-popn (->starting-popn data-inputs params)]
      (is (= nil get-start-popn)))))

(deftest core-loop-test
  (testing "The loop is functional"
    (let [exec-loop (core-loop data-inputs params)]
      (is (= nil exec-loop)))))
