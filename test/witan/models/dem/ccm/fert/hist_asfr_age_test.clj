(ns witan.models.dem.ccm.fert.hist-asfr-age-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.fert.hist-asfr-age :refer :all]
            [clojure.string :as str]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

;; Load testing data
(def test-data-paths {:births-data "resources/test_data/bristol_births_data.csv"
                      :at-risk-popn "resources/test_data/bristol_denominators.csv"
                      :mye-coc "resources/test_data/bristol_mye_coc.csv"})

(def data-inputs (ld/load-datasets test-data-paths))

(def params {:fert-last-yr 2014})

;; Functions take maps of all inputs/outputs from parent nodes in workflow
(def from-births-data-year (merge data-inputs
                                  (->births-data-year data-inputs params)))
(def for-births-pool (merge from-births-data-year
                            (->at-risk-this-year from-births-data-year params)
                            (->at-risk-last-year from-births-data-year params)))

(def from-births-pool (merge for-births-pool
                             (->births-pool for-births-pool params)))
;; End of input data handling

(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

;; Tests
(deftest ->births-data-year-test
  (testing "The latest year is returned"
    (is (= 2013
           (:yr (->births-data-year data-inputs params))))))

(deftest ->at-risk-this-year-test
  (testing "The data transformation returns the correct columns"
    (is (same-coll? [:gss-code :sex :popn-this-yr :age]
                    (ds/column-names (:at-risk-this-year
                                      (->at-risk-this-year from-births-data-year params))))))
  (testing "The age column range is now 0-89 instead of 1-90"
    (let [former-age-range (distinct (i/$ :age (:at-risk-popn data-inputs)))
          min-former-range (reduce min former-age-range)
          max-former-range (reduce max former-age-range)]
      (is (same-coll? (range (dec min-former-range) max-former-range)
                      (distinct (i/$ :age (:at-risk-this-year
                                           (->at-risk-this-year from-births-data-year
                                                                params)))))))))

(deftest ->at-risk-last-year-test
  (testing "The data is filtered by the correct year"
    (is (same-coll? '(2013)
                    (distinct (i/$ :year (:at-risk-last-year
                                          (->at-risk-last-year from-births-data-year params)))))))
  (testing "The data transformation returns the correct columns"
    (is (same-coll? [:gss-code :sex :age :year :popn-last-yr]
                    (ds/column-names (:at-risk-last-year
                                      (->at-risk-last-year from-births-data-year params)))))))

(deftest ->births-pool-test
  (testing "The data transformation returns the correct columns"
    (is (same-coll? [:age :sex :gss-code :year :birth-pool]
                    (ds/column-names (:births-pool (->births-pool for-births-pool params))))))
  (testing "No nil or NaN in the birth-pool column"
    (is (not-any? nil? (i/$ :birth-pool (:births-pool (->births-pool for-births-pool params)))))
    (is (not-any? #(Double/isNaN %)
                  (i/$ :birth-pool (:births-pool (->births-pool for-births-pool params)))))))

(deftest ->historic-fertility-test
  (testing "The intermediary outputs are added to the global map"
    (let [hist-asfr (->historic-fertility data-inputs params)]
      (is (contains? hist-asfr :yr))
      (is (contains? hist-asfr :at-risk-this-year))
      (is (contains? hist-asfr :at-risk-last-year))
      (is (contains? hist-asfr :births-pool)))))
