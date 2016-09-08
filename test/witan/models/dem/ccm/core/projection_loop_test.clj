(ns witan.models.dem.ccm.core.projection-loop-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.core.projection-loop :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [witan.models.load-data :as ld]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.fert.fertility :as fert]
            [witan.models.dem.ccm.mort.mortality :as mort]
            [witan.models.dem.ccm.mig.migration :as mig]))

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
                   "./datasets/test_datasets/model_inputs/mort/death_improvement.csv"}))

(def params {;; Core module
             :first-proj-year 2014
             :last-proj-year 2016
             ;; Fertility module
             :fert-base-year 2014
             :proportion-male-newborns (double (/ 105 205))
             ;; Mortality module
             ;; (s/validate (s/pred (>= % earliest-mort-year)) :start-year-avg-mort)
             :start-year-avg-mort 2010
             ;; (s/validate (s/pred (<= % (dec jumpoff-year-mort))) :end-year-avg-mort)
             :end-year-avg-mort 2014
             :variant :average-fixed
             ;; Migration module
             ;; (s/validate (s/pred (>= % earliest-domin-mig-year)) :start-year-avg-domin-mig)
             :start-year-avg-domin-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-year-mig))) :end-year-avg-domin-mig)
             :end-year-avg-domin-mig 2014
             ;; (s/validate (s/pred (>= % earliest-domout-mig-year)) :start-year-avg-domout-mig)
             :start-year-avg-domout-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-year-mig))) :end-year-avg-domout-mig)
             :end-year-avg-domout-mig 2014
             ;; (s/validate (s/pred (>= % earliest-intin-mig-year)) :start-year-avg-intin-mig)
             :start-year-avg-intin-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-year-mig))) :end-year-avg-intin-mig)
             :end-year-avg-intin-mig 2014
             ;; (s/validate (s/pred (>= % earliest-intout-mig-year)) :start-year-avg-intout-mig)
             :start-year-avg-intout-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-year-mig))) :end-year-avg-intout-mig)
             :end-year-avg-intout-mig 2014})

(def prepared-inputs (prepare-inputs data-inputs))

(defn fertility-module [inputs params]
  (-> inputs
      (fert/calculate-historic-asfr params)
      fert/project-asfr-1-0-0
      fert/project-births-1-0-0
      (fert/combine-into-births-by-sex params)))

(defn fertility-module-1 [inputs params]
  (-> inputs
      (fert/calculate-historic-asfr params)))

(defn mortality-module [inputs params]
  (-> inputs
      mort/calc-historic-asmr
      (mort/project-asmr-1-0-0 params)
      mort/project-deaths))

(defn migration-module [inputs params]
  (-> inputs
      (mig/project-domestic-in-migrants params)
      (mig/project-domestic-out-migrants params)
      (mig/project-international-in-migrants params)
      (mig/project-international-out-migrants params)
      mig/combine-into-net-flows))

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
                      (ds/column-names (:latest-year-popn get-start-popn))))
      (is (true? (every? #(= % 2015) (ds/column (:latest-year-popn get-start-popn) :year))))
      ;; Total number of 15 years old in 2015:
      (is (= 4386.0 (apply + (ds/column (wds/select-from-ds (:latest-year-popn get-start-popn)
                                                            {:year 2015 :age 15}) :popn))))
      ;; Total number of women aged between 40 and 43 in 2015:
      (is (= 5394.0 (apply + (ds/column (wds/select-from-ds (:latest-year-popn get-start-popn)
                                                            {:sex "F" :age {:$gt 40 :lt 43}
                                                             :year 2015}) :popn)))))))

(deftest age-on-test
  (testing "Popn is aged on correctly, 90 grouping is handled correctly."
    (let [starting-popn (:latest-year-popn (select-starting-popn prepared-inputs))
          aged-popn (:latest-year-popn (age-on (select-starting-popn prepared-inputs)
                                               params))]
      (is (= (sum-popn starting-popn 2015 15)
             (sum-popn aged-popn 2015 16)))
      (is (= (+ (sum-popn starting-popn 2015 89) (sum-popn starting-popn 2015 90))
             (sum-popn aged-popn 2015 90)))
      (is (= 2 (first (:shape (wds/select-from-ds aged-popn {:year 2015 :age 90}))))))))

(deftest add-births-test
  (testing "Newborns are correctly added to projection year popn."
    (let [births-added (-> prepared-inputs
                           select-starting-popn
                           (fertility-module params)
                           age-on
                           add-births)
          popn-with-births (:latest-year-popn births-added)
          latest-year (:loop-year births-added)
          latest-newborns (wds/select-from-ds popn-with-births {:year latest-year :age 0})]
      (is (= 2 (first (:shape latest-newborns))))
      (is (fp-equals? (+ 3189.57438270303 3349.0531018318)
                      (#(apply + (ds/column % :popn)) latest-newborns) 0.0001)))))

(deftest remove-deaths-test
  (testing "The deaths are removed from the popn."
    (let [aged-on-popn (-> prepared-inputs
                           select-starting-popn
                           (fertility-module params)
                           (mortality-module params)
                           age-on)
          popn-with-births (add-births aged-on-popn)
          popn-wo-deaths (remove-deaths popn-with-births)]
      (is (= (apply - (concat (get-popn (:latest-year-popn popn-with-births) :popn 2015 0 "F")
                              (get-popn (:deaths popn-with-births) :deaths 0 "F")))
             (nth (get-popn (:latest-year-popn popn-wo-deaths) :popn 2015 0 "F") 0))))))

(deftest apply-migration-test
  (testing "The migrants are added to the popn."
    (let [popn-with-births (-> prepared-inputs
                               select-starting-popn
                               (fertility-module params)
                               (mortality-module params)
                               (migration-module params)
                               age-on
                               add-births)
          popn-wo-deaths (remove-deaths popn-with-births)
          popn-with-mig (apply-migration popn-wo-deaths)]
      (is (= (apply + (concat  (get-popn (:latest-year-popn popn-wo-deaths) :popn 2015 0 "F")
                               (get-popn (:net-migration popn-wo-deaths) :net-mig 0 "F")))
             (nth (get-popn (:latest-year-popn popn-with-mig) :popn 2015 0 "F") 0))))))
