(ns witan.models.dem.ccm.fert.fertility-mvp-test
  (:require [witan.models.dem.ccm.fert.fertility-mvp :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;;;;;;;;;;;;;;;;
;; Test inputs ;;
;;;;;;;;;;;;;;;;;

;; historic-asmr = output of calc historic ASMR fn from R, to use until
;;                 a Clojure version exists

;;proj-births-by-age-sex-mother = output of project.births.from.fixed.rates
;;                             fn from R in fertility module, to use
;;                             until a Clojure version exists

(def fertility-inputs (-> {:base-asfr
                           "dev-resources/test_data/fertility/bristol_base_ASFR.csv"
                           :historic-total-births
                           "dev-resources/test_data/bristol_hist_births_mye.csv"
                           :historic-population
                           "dev-resources/test_data/bristol_hist_popn_mye.csv"
                           :population
                           "dev-resources/test_data/handmade_outputs/bristol_popn_at_risk_2015.csv"}
                          ld/load-datasets
                          (clojure.set/rename-keys {:population :population-at-risk})))

(def params {:fert-last-yr 2014 :pm (double (/ 105 205))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; R outputs for comparison ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def fertility-outputs-r (ld/load-datasets
                          {:projected-asfr-finalyrfixed
                           "dev-resources/test_data/fertility/bristol_initial_projected_fertility_rates_2015.csv"
                           :births-by-sex
                           "dev-resources/test_data/fertility/bristol_proj_births_by_sex_2015.csv"
                           :historic-asfr
                           "dev-resources/test_data/fertility/bristol_historic_asfr.csv"
                           :births-by-age-sex-mother
                           "dev-resources/test_data/fertility/bristol_proj_births_age_sex_mother_2015.csv"}))

(deftest calculate-historic-asfr-test
  (testing "Historic ASFR calculation"
    (let [calc-hist-asfr (:historic-asfr
                          (calculate-historic-asfr fertility-inputs params))
          r-output-hist-asfr (ds/rename-columns
                              (:historic-asfr fertility-outputs-r)
                              {:fert-rate :fert-rate-r})
          joined-hist-asfr (wds/join calc-hist-asfr r-output-hist-asfr
                                     [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-hist-asfr :rows % :cols :fert-rate)
                               (i/sel joined-hist-asfr :rows % :cols :fert-rate-r)
                               0.0000000001)
                  (range (first (:shape joined-hist-asfr))))))))

(deftest project-asfr-finalyrhist-fixed-test
  (testing "Fertility rates are projected correctly."
    (let [proj-asfr-r (-> fertility-outputs-r
                          :projected-asfr-finalyrfixed
                          (ds/rename-columns {:fert-rate :fert-rate-r}))
          joined-asfr (-> fertility-inputs
                          (calculate-historic-asfr params)
                          (project-asfr-finalyrhist-fixed params)
                          :initial-projected-fertility-rates
                          (wds/join proj-asfr-r [:gss-code :sex :age]))]
      (is (every? #(fp-equals? (i/sel joined-asfr :rows % :cols :fert-rate-r)
                               (i/sel joined-asfr :rows % :cols :fert-rate)
                               0.0000000001)
                  (range (first (:shape joined-asfr))))))))

(deftest project-births-from-fixed-rates-test
  (testing "Projected births match R values"
    (let [proj-births-r (-> fertility-outputs-r
                            :births-by-age-sex-mother
                            (ds/rename-columns {:births :births-r}))
          joined-births (-> fertility-inputs
                            (calculate-historic-asfr params)
                            (project-asfr-finalyrhist-fixed params)
                            project-births-from-fixed-rates
                            :births-by-age-sex-mother
                            (wds/join proj-births-r [:gss-code :sex :age :year]))]
      (is (every? #(fp-equals? (i/sel joined-births :rows % :cols :births-r)
                               (i/sel joined-births :rows % :cols :births)
                               0.00001)
                  (range (first (:shape joined-births))))))))

(deftest combine-into-births-by-sex-test
  (testing "Births by sex have been gathered correctly & match R output"
    (let [births-by-sex-r (-> fertility-outputs-r
                              :births-by-sex
                              (ds/rename-columns {:births :births-r}))
          joined-births-by-sex (-> fertility-inputs
                                   (calculate-historic-asfr params)
                                   (project-asfr-finalyrhist-fixed params)
                                   project-births-from-fixed-rates
                                   (combine-into-births-by-sex params)
                                   :births-by-sex
                                   (wds/join births-by-sex-r [:gss-code :sex]))]
      (is (every? #(fp-equals? (i/sel joined-births-by-sex :rows % :cols :births-r)
                               (i/sel joined-births-by-sex :rows % :cols :births)
                               0.00000001)
                  (range (first (:shape joined-births-by-sex))))))))
