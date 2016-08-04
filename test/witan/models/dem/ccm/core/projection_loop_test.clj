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
             :fert-base-yr 2014
             :proportion-male-newborns (double (/ 105 205))
             ;; Mortality module
             ;; (s/validate (s/pred (>= % earliest-mort-yr)) :start-yr-avg-mort)
             :start-yr-avg-mort 2010
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mort))) :end-yr-avg-mort)
             :end-yr-avg-mort 2014
             ;; Migration module
             ;; (s/validate (s/pred (>= % earliest-domin-mig-yr)) :start-yr-avg-domin-mig)
             :start-yr-avg-domin-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mig))) :end-yr-avg-domin-mig)
             :end-yr-avg-domin-mig 2014
             ;; (s/validate (s/pred (>= % earliest-domout-mig-yr)) :start-yr-avg-domout-mig)
             :start-yr-avg-domout-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mig))) :end-yr-avg-domout-mig)
             :end-yr-avg-domout-mig 2014
             ;; (s/validate (s/pred (>= % earliest-intin-mig-yr)) :start-yr-avg-intin-mig)
             :start-yr-avg-intin-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mig))) :end-yr-avg-intin-mig)
             :end-yr-avg-intin-mig 2014
             ;; (s/validate (s/pred (>= % earliest-intout-mig-yr)) :start-yr-avg-intout-mig)
             :start-yr-avg-intout-mig 2003
             ;; (s/validate (s/pred (<= % (dec jumpoff-yr-mig))) :end-yr-avg-intout-mig)
             :end-yr-avg-intout-mig 2014})

(def prepared-inputs (prepare-inputs data-inputs))

(defn fertility-module [inputs params]
  (-> inputs
      (fert/calculate-historic-asfr params)
      fert/project-asfr-finalyrhist-fixed
      fert/project-births-from-fixed-rates
      (fert/combine-into-births-by-sex params)))

(defn mortality-module [inputs params]
  (-> inputs
      mort/calc-historic-asmr
      (mort/project-asmr-average-fixed params)
      mort/project-deaths-from-fixed-rates))

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
                      (ds/column-names (:latest-yr-popn get-start-popn))))
      (is (true? (every? #(= % 2015) (ds/column (:latest-yr-popn get-start-popn) :year))))
      ;; Total number of 15 yrs old in 2015:
      (is (= 4386.0 (apply + (ds/column (wds/select-from-ds (:latest-yr-popn get-start-popn)
                                                         {:year 2015 :age 15}) :popn))))
      ;; Total number of women aged between 40 and 43 in 2015:
      (is (= 5394.0 (apply + (ds/column (wds/select-from-ds (:latest-yr-popn get-start-popn)
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
      (is (= 2 (first (:shape (wds/select-from-ds aged-popn {:year 2015 :age 90}))))))))

(deftest add-births-test
  (testing "Newborns are correctly added to projection year popn."
    (let [births-added (-> prepared-inputs
                           select-starting-popn
                           (fertility-module params)
                           age-on
                           add-births)
          popn-with-births (:latest-yr-popn births-added)
          latest-yr (:loop-year births-added)
          latest-newborns (wds/select-from-ds popn-with-births {:year latest-yr :age 0})]
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
      (is (= (apply - (concat (get-popn (:latest-yr-popn popn-with-births) :popn 2015 0 "F")
                              (get-popn (:deaths popn-with-births) :deaths 0 "F")))
             (nth (get-popn (:latest-yr-popn popn-wo-deaths) :popn 2015 0 "F") 0))))))

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
      (is (= (apply + (concat  (get-popn (:latest-yr-popn popn-wo-deaths) :popn 2015 0 "F")
                               (get-popn (:net-migration popn-wo-deaths) :net-mig 0 "F")))
             (nth (get-popn (:latest-yr-popn popn-with-mig) :popn 2015 0 "F") 0))))))
