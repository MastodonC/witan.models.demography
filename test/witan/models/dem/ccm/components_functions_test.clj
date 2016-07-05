(ns witan.models.dem.ccm.components-functions-test
  (:require [witan.models.dem.ccm.components-functions :refer :all]
            [clojure.test :refer :all]
            [incanter.core :as i]
            [witan.models.load-data :as ld]
            [witan.datasets :as wds]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;Historical migration data
(def migration-data (ld/load-datasets {:domestic-in-migrants
                                       "test_data/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                       :domestic-out-migrants
                                       "test_data/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                       :international-in-migrants
                                       "test_data/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                       :international-out-migrants
                                       "test_data/model_inputs/mig/bristol_hist_international_outmigrants.csv"}))

;;Following 4 datasets are averages of historical migration data calculated in the R model
;;using the equivalent of the jumpoffyr-method-average function
(def dom-in-averages (ld/load-dataset :dom-in-averages
                                      "test_data/r_outputs_for_testing/mig/bristol_proj_domestic_inmigrants_valuesavgfixed.csv"))

(def dom-out-averages (ld/load-dataset :dom-out-averages
                                       "test_data/r_outputs_for_testing/mig/bristol_proj_domestic_outmigrants_valuesavgfixed.csv"))

(def inter-in-averages (ld/load-dataset :inter-in-averages
                                        "test_data/r_outputs_for_testing/mig/bristol_proj_international_inmigrants_valuesavgfixed.csv"))

(def inter-out-averages (ld/load-dataset :inter-out-averages
                                         "test_data/r_outputs_for_testing/mig/bristol_proj_international_outmigrants_valuesavgfixed.csv"))

(deftest jumpoffyr-method-average-test
  (testing "The function return the averages on the right year period"
    (let [r-results (:dom-in-averages dom-in-averages)
          clj-results (jumpoffyr-method-average (:domestic-in-migrants migration-data)
                                                :estimate :domestic-in 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :domin)
                               (i/sel joined-averages :rows % :cols :domestic-in) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:dom-out-averages dom-out-averages)
          clj-results (jumpoffyr-method-average (:domestic-out-migrants migration-data)
                                                :estimate :domestic-out 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :domout)
                               (i/sel joined-averages :rows % :cols :domestic-out) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:inter-out-averages inter-out-averages)
          clj-results (jumpoffyr-method-average (:international-out-migrants migration-data)
                                                :estimate :international-out 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :intout)
                               (i/sel joined-averages :rows % :cols :international-out) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:inter-in-averages inter-in-averages)
          clj-results (jumpoffyr-method-average (:international-in-migrants migration-data)
                                                :estimate :international-in 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :intin)
                               (i/sel joined-averages :rows % :cols :international-in) 0.0000000001)
                  (range (first (:shape joined-averages))))))))
