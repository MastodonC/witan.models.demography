(ns witan.models.dem.ccm.core.projection-loop-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.core.projection-loop :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.fert.fertility-mvp :as fert]
            [witan.models.dem.ccm.mort.mortality-mvp :as mort]
            [witan.models.dem.ccm.mig.net-migration :as mig]))

;; Load testing data
(def data-inputs (ld/load-datasets
                  {:ons-proj-births-by-age-mother
                   "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                   :historic-births
                   "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"
                   :historic-population
                   "./datasets/test_datasets/model_inputs/bristol_hist_popn_mye.csv"
                   :historic-deaths
                   "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"
                   :domestic-in-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                   :domestic-out-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                   :international-in-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                   :international-out-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"}))

(def params {;; Core module
             :first-proj-year 2014
             :last-proj-year 2015
             ;; Fertility module
             :fert-last-yr 2014
             :start-yr-avg-fert 2014
             :end-yr-avg-fert 2014 ;; (s/validate (s/eq :fert-last-yr) :end-yr-avg-fert)
             :proportion-male-newborns (double (/ 105 205))
             ;; Mortality module
             ;; (s/validate (s/pred (>= % earliest-mort-yr)) :start-yr-avg-mort)
             :start-yr-avg-mort 2010
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mort))) :end-yr-avg-mort)
             :end-yr-avg-mort 2014
             ;; Migration module
             ;; (s/validate (s/pred (>= % earliest-dom-mig-yr)) :start-yr-avg-dom-mig)
             :start-yr-avg-dom-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mig))) :end-yr-avg-dom-mig)
             :end-yr-avg-dom-mig 2014
             ;; (s/validate (s/pred (>= % earliest-inter-mig-yr)) :start-yr-avg-inter-mig)
             :start-yr-avg-inter-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mig))) :end-yr-avg-inter-mig)
             :end-yr-avg-inter-mig 2014})

(def params-2040 {;; Core module
                  :first-proj-year 2014
                  :last-proj-year 2040
                  ;; Fertility module
                  :fert-last-yr 2014
                  :start-yr-avg-fert 2014
                  :end-yr-avg-fert 2014
                  :proportion-male-newborns (double (/ 105 205))
                  ;; Mortality module
                  :start-yr-avg-mort 2010
                  :end-yr-avg-mort 2014
                  ;; Migration module
                  :start-yr-avg-dom-mig 2003
                  :end-yr-avg-dom-mig 2014
                  :start-yr-avg-inter-mig 2003
                  :end-yr-avg-inter-mig 2014})

(def prepared-inputs (-> data-inputs
                         prepare-inputs
                         (fert/fertility-pre-projection params)
                         (mort/mortality-pre-projection params)
                         (mig/migration-pre-projection params)))

(def output-2015 (ld/load-datasets
                  {:end-population
                   "./datasets/test_datasets/r_outputs_for_testing/core/bristol_end_population_2015.csv"}))

(def output-2040 (ld/load-datasets
                  {:end-population
                   "./datasets/test_datasets/r_outputs_for_testing/core/bristol_end_population_2040.csv"}))

;; Useful fns:
(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;get sum of population for particular year & age group in dataset with :popn column
(def sum-popn (fn [popn year age]
                (apply + (ds/column (i/query-dataset popn {:year year :age age}) :popn))))

;;returns a vector
(def get-popn (fn
                ([ds col-name age sex]
                 (ds/column (i/query-dataset ds {:age age :sex sex}) col-name))
                ([ds col-name year age sex]
                 (ds/column (i/query-dataset ds {:year year :age age :sex sex}) col-name))))

;; Tests:
(deftest select-starting-popn-test
  (testing "The starting popn is returned"
    (let [get-start-popn (select-starting-popn prepared-inputs)]
      (is (same-coll? [:gss-code :sex :age :year :popn]
                      (ds/column-names (:latest-yr-popn get-start-popn))))
      (is (true? (every? #(= % 2015) (ds/column (:latest-yr-popn get-start-popn) :year))))
      ;; Total number of 15 yrs old in 2015:
      (is (= 4386.0 (apply + (ds/column (i/query-dataset (:latest-yr-popn get-start-popn)
                                                         {:year 2015 :age 15}) :popn))))
      ;; Total number of women aged between 40 and 43 in 2015:
      (is (= 5394.0 (apply + (ds/column (i/query-dataset (:latest-yr-popn get-start-popn)
                                                         {:sex "F" :age {:$gt 40 :lt 43}
                                                          :year 2015}) :popn)))))))

(deftest age-on-test
  (testing "Popn is aged on correctly, 90 grouping is handled correctly."
    (let [starting-popn (:latest-yr-popn (select-starting-popn prepared-inputs))
          aged-popn (:latest-yr-popn (age-on (select-starting-popn prepared-inputs)
                                             params))]
      (is (= (sum-popn starting-popn 2015 15)
             (sum-popn aged-popn 2015 16)))
      (is (= (+ (sum-popn starting-popn 2015 89) (sum-popn starting-popn 2015 90))
             (sum-popn aged-popn 2015 90)))
      (is (= 2 (first (:shape (i/query-dataset aged-popn {:year 2015 :age 90}))))))))

(deftest add-births-test
  (testing "Newborns are correctly added to projection year popn."
    (let [births-added (-> prepared-inputs
                           select-starting-popn
                           (fert/births-projection params)
                           age-on
                           add-births)
          popn-with-births (:latest-yr-popn births-added)
          latest-yr (:loop-year births-added)
          latest-newborns (i/query-dataset popn-with-births {:year latest-yr :age 0})]
      (is (= 2 (first (:shape latest-newborns))))
      (is (fp-equals? (+ 3189.57438270303 3349.0531018318)
                      (#(apply + (ds/column % :popn)) latest-newborns) 0.0001)))))

(deftest remove-deaths-test
  (testing "The deaths are removed from the popn."
    (let [aged-on-popn (-> prepared-inputs
                           select-starting-popn
                           (fert/births-projection params)
                           mort/project-deaths-from-fixed-rates
                           age-on)
          popn-with-births (add-births aged-on-popn)
          popn-wo-deaths (remove-deaths popn-with-births)]
      (is (= (apply - (concat (get-popn (:latest-yr-popn popn-with-births) :popn 2015 0 "F")
                              (get-popn (:deaths popn-with-births) :deaths 0 "F")))
             (nth (get-popn (:latest-yr-popn popn-wo-deaths) :popn 2015 0 "F") 0))))))

(deftest apply-migration-test
  (testing "The migrants are added to the popn."
    (let [popn-with-births (-> prepared-inputs
                               select-starting-popn
                               (fert/births-projection params)
                               mort/project-deaths-from-fixed-rates
                               age-on
                               add-births)
          popn-wo-deaths (remove-deaths popn-with-births)
          popn-with-mig (apply-migration popn-wo-deaths)]
      (is (= (apply + (concat  (get-popn (:latest-yr-popn popn-wo-deaths) :popn 2015 0 "F")
                               (get-popn (:net-migration popn-wo-deaths) :net-mig 0 "F")))
             (nth (get-popn (:latest-yr-popn popn-with-mig) :popn 2015 0 "F") 0))))))

(deftest looping-test-test
  (testing "The output of the loop matches the R code."
    (let [proj-clj (looping-test data-inputs params-2040)
          proj-clj-2015 (ds/rename-columns
                         (i/query-dataset proj-clj
                                          {:year
                                           {:$eq 2015}})
                         {:popn :popn-clj})
          proj-clj-2040 (ds/rename-columns
                         (i/query-dataset proj-clj
                                          {:year
                                           {:$eq 2040}})
                         {:popn :popn-clj})
          proj-r-2015 (ds/rename-columns
                       (:end-population output-2015) {:popn :popn-r})
          proj-r-2040 (ds/rename-columns
                       (:end-population output-2040) {:popn :popn-r})
          r-clj-2015 (wds/join proj-r-2015 proj-clj-2015 [:gss-code :sex :age :year])
          r-clj-2040 (wds/join proj-r-2040 proj-clj-2040 [:gss-code :sex :age :year])]
      ;; Compare all values in the :popn column between the R and Clj results for 2015:
      (is (every? #(fp-equals? (i/sel r-clj-2015 :rows % :cols :popn-r)
                               (i/sel r-clj-2015 :rows % :cols :popn-clj) 0.0001)
                  (range (first (:shape r-clj-2015)))))
      ;; Compare all values in the :popn column between the R and Clj results for 2040:
      (is (every? #(fp-equals? (i/sel r-clj-2040 :rows % :cols :popn-r)
                               (i/sel r-clj-2040 :rows % :cols :popn-clj) 0.0001)
                  (range (first (:shape r-clj-2040))))))))
