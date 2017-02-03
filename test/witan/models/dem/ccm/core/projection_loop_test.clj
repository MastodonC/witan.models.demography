(ns witan.models.dem.ccm.core.projection-loop-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.core.projection-loop :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [witan.models.load-data :as ld]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.fert.fertility :as fert]
            [witan.models.dem.ccm.mort.mortality :as mort]
            [witan.models.dem.ccm.mig.migration :as mig]
            [witan.workspace-api :refer [merge->]]))

;; Load testing data
(def data-inputs (ld/load-datasets
                  {:historic-births-by-age-mother
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
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                   :future-mortality-trend-assumption
                   "./datasets/test_datasets/model_inputs/mort/death_improvement.csv"
                   :future-fertility-trend-assumption
                   "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"}))

(def params {;; Core module
             :first-proj-year 2015
             :last-proj-year 2016
             ;; Fertility module
             :proportion-male-newborns (double (/ 105 205))
             :proj-asfr-variant :fixed
             :future-fert-scenario :principal-2012
             :fert-base-year 2014
             ;; Mortality module
             ;; (s/validate (s/pred (>= % earliest-mort-year)) :start-year-avg-mort)
             :start-year-avg-mort 2010
             :end-year-avg-mort 2014
             :proj-asmr-variant :average-fixed
             :mort-scenario :principal
             ;; (s/validate (s/pred (<= % (dec first-projection-year-mort))) :end-year-avg-mort)
             ;; Migration module
             ;; (s/validate (s/pred (>= % earliest-domin-mig-year)) :start-year-avg-domin-mig)
             :start-year-avg-domin-mig 2003
             ;; (s/validate (s/pred (<= % (dec first-projection-year-mig))) :end-year-avg-domin-mig)
             :end-year-avg-domin-mig 2014
             ;; (s/validate (s/pred (>= % earliest-domout-mig-year)) :start-year-avg-domout-mig)
             :start-year-avg-domout-mig 2003
             ;; (s/validate (s/pred (<= % (dec first-projection-year-mig))) :end-year-avg-domout-mig)
             :end-year-avg-domout-mig 2014
             ;; (s/validate (s/pred (>= % earliest-intin-mig-year)) :start-year-avg-intin-mig)
             :start-year-avg-intin-mig 2003
             ;; (s/validate (s/pred (<= % (dec first-projection-year-mig))) :end-year-avg-intin-mig)
             :end-year-avg-intin-mig 2014
             ;; (s/validate (s/pred (>= % earliest-intout-mig-year)) :start-year-avg-intout-mig)
             :start-year-avg-intout-mig 2003
             ;; (s/validate (s/pred (<= % (dec first-projection-year-mig))) :end-year-avg-intout-mig)
             :end-year-avg-intout-mig 2014
             :brexit-parameter 1})

(def prepared-inputs (prepare-inputs-1-0-0 data-inputs params))

(defn fertility-module [inputs params]
  (as-> inputs x
    (fert/calculate-historic-asfr    (merge x inputs data-inputs) params)
    (fert/project-asfr-1-0-0         (merge x inputs data-inputs) params)
    (fert/project-births-1-0-0       (merge x inputs data-inputs))
    (fert/combine-into-births-by-sex (merge x inputs data-inputs) params)))

(defn fertility-module-1 [inputs params]
  (-> inputs
      (fert/calculate-historic-asfr params)))

(defn mortality-module [inputs params]
  (as-> inputs x
    (mort/calculate-historic-asmr (merge x inputs data-inputs))
    (mort/project-asmr-1-0-0 (merge x inputs data-inputs) params)
    (mort/project-deaths-1-0-0 (merge x inputs data-inputs))))

(defn migration-module [inputs params]
  (-> inputs
      (merge data-inputs)
      (merge->
       (mig/projected-domestic-in-migrants params)
       (mig/projected-domestic-out-migrants params)
       (mig/projected-international-in-migrants params)
       (mig/projected-international-out-migrants params)
       (select-starting-popn prepared-inputs))
      (mig/combine-into-net-flows)))

;; Useful fns:
(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;get sum of population for particular year & age group in dataset with :popn column
(def sum-popn (fn [popn year age]
                (apply + (ds/column (wds/select-from-ds popn {:year year :age age}) :popn))))

;;returns a vector
(def get-popn (fn
                ([ds col-name age sex]
                 (ds/column (wds/select-from-ds ds {:age age :sex sex}) col-name))
                ([ds col-name year age sex]
                 (ds/column (wds/select-from-ds ds {:year year :age age :sex sex}) col-name))))

;; Tests:
(deftest select-starting-popn-test
  (testing "The starting popn is returned"
    (let [get-start-popn (select-starting-popn prepared-inputs)]
      (is (same-coll? [:gss-code :sex :age :year :popn]
                      (ds/column-names (:current-year-popn get-start-popn))))
      (is (true? (every? #(= % 2015) (ds/column (:current-year-popn get-start-popn) :year))))
      ;; Total number of 15 years old in 2015:
      (is (= 4386.0 (apply + (ds/column (wds/select-from-ds (:current-year-popn get-start-popn)
                                                            {:year 2015 :age 15}) :popn))))
      ;; Total number of women aged between 40 and 43 in 2015:
      (is (= 5394.0 (apply + (ds/column (wds/select-from-ds (:current-year-popn get-start-popn)
                                                            {:sex "F" :age {:$gt 40 :lt 43}
                                                             :year 2015}) :popn)))))))

(deftest age-on-test
  (testing "Popn is aged on correctly, 90 grouping is handled correctly."
    (let [starting-popn (:current-year-popn (select-starting-popn prepared-inputs))
          aged-popn (:current-year-popn (age-on (select-starting-popn prepared-inputs)
                                               params))]
      (is (= (sum-popn starting-popn 2015 15)
             (sum-popn aged-popn 2015 16)))
      (is (= (+ (sum-popn starting-popn 2015 89) (sum-popn starting-popn 2015 90))
             (sum-popn aged-popn 2015 90)))
      (is (= 2 (first (:shape (wds/select-from-ds aged-popn {:year 2015 :age 90}))))))))

(deftest add-births-test
  (testing "Newborns are correctly added to projection year popn."
    (let [start-popn       (select-starting-popn prepared-inputs)
          fertility        (-> start-popn
                               (merge prepared-inputs)
                               (fertility-module params))
          aged-on          (-> start-popn
                               (merge prepared-inputs)
                               age-on)
          births-added     (-> aged-on
                               (merge fertility)
                               add-births)
          popn-with-births (:current-year-popn births-added)
          latest-year      (first (ds/column popn-with-births :year))
          latest-newborns  (wds/select-from-ds popn-with-births {:year latest-year :age 0})]
      (is (= 2 (first (:shape latest-newborns))))
      (is (fp-equals? (+ 3189.57438270303 3349.0531018318)
                      (#(apply + (ds/column % :popn)) latest-newborns) 0.0001)))))

(deftest remove-deaths-test
  (testing "The deaths are removed from the popn."
    (let [start-popn       (select-starting-popn prepared-inputs)
          fertility        (-> start-popn
                               (merge prepared-inputs)
                               (fertility-module params))
          mortality        (-> start-popn
                               (merge prepared-inputs)
                               (mortality-module params))
          aged-on          (-> start-popn
                               (merge prepared-inputs)
                               age-on)
          popn-with-births (add-births (merge fertility aged-on))
          popn-wo-deaths (remove-deaths (merge mortality popn-with-births))]
      (is (= (apply - (concat (get-popn (:current-year-popn popn-with-births) :popn 2015 0 "F")
                              (get-popn (:deaths mortality) :deaths 0 "F")))
             (nth (get-popn (:current-year-popn popn-wo-deaths) :popn 2015 0 "F") 0))))))

(deftest apply-migration-test
  (testing "The migrants are added to the popn."
    (let [
          start-popn       (select-starting-popn prepared-inputs)
          fertility        (-> start-popn
                               (merge prepared-inputs)
                               (fertility-module params))
          mortality        (-> start-popn
                               (merge prepared-inputs)
                               (mortality-module params))
          migration        (-> start-popn
                               (merge prepared-inputs)
                               (migration-module params))
          aged-on          (-> start-popn
                               (merge prepared-inputs)
                               age-on)
          popn-with-births (add-births (merge fertility aged-on))
          popn-wo-deaths (remove-deaths (merge mortality popn-with-births))
          popn-with-mig (apply-migration (merge migration popn-wo-deaths))]
      (is (= (apply + (concat  (get-popn (:current-year-popn popn-wo-deaths) :popn 2015 0 "F")
                               (get-popn (:net-migration migration) :net-mig 0 "F")))
             (nth (get-popn (:current-year-popn popn-with-mig) :popn 2015 0 "F") 0))))))
