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

;;proj-births-age-sex-mother = output of project.births.from.fixed.rates
;;                             fn from R in fertility module, to use
;;                             until a Clojure version exists

(def fertility-inputs (ld/load-datasets
                       {:historic-asfr
                        "resources/test_data/fertility/bristol_historic_asfr.csv"
                        :births-age-sex-mother
                        "resources/test_data/fertility/bristol_proj_births_age_sex_mother_2015.csv"}))

(def params {:fert-last-yr 2014 :pm (/ 105 205)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; R outputs for comparison ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def fertility-outputs-r (ld/load-datasets
                          {:projected-asfr-finalyrfixed
                           "resources/test_data/fertility/bristol_initial_projected_fertility_rates_2015.csv"
                           :births-by-sex
                           "resources/test_data/fertility/bristol_proj_births_by_sex_2015.csv"}))

(deftest project-asfr-finalyrhist-fixed-test
  (testing "Fertiliy rates are projected correctly."
    (let [proj-asfr-r (-> fertility-outputs-r
                          :projected-asfr-finalyrfixed
                          (ds/rename-columns {:fert-rate :fert-rate-r}))
          joined-asfr (-> fertility-inputs
                          (project-asfr-finalyrhist-fixed params)
                          :initial-projected-fertility-rates
                          (wds/join proj-asfr-r [:gss-code :sex :age]))]
      (is (every? #(fp-equals? (i/sel joined-asfr :rows % :cols :fert-rate-r)
                               (i/sel joined-asfr :rows % :cols :fert-rate)
                               0.0000000001)
                  (range (first (:shape joined-asfr))))))))


(deftest combine-into-births-by-sex-test
  (testing "Births by sex have been gathered correctly & match R output"
    (let [births-by-sex-r (-> fertility-outputs-r
                              :births-by-sex
                              (ds/rename-columns {:births :births-r}))
          joined-births-by-sex (-> fertility-inputs
                                   :births-age-sex-mother
                                   (combine-into-births-by-sex (:pm params))
                                   (wds/join births-by-sex-r [:gss-code :sex]))]
      (is (every? #(fp-equals? (i/sel joined-births-by-sex :rows % :cols :births-r)
                               (i/sel joined-births-by-sex :rows % :cols :births)
                               0.00000001)
                  (range (first (:shape joined-births-by-sex))))))))
