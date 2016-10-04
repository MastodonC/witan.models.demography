(ns witan.models.dem.ccm.components-functions-test
  (:require [witan.models.dem.ccm.components-functions :refer :all]
            [clojure.test :refer :all]
            [witan.models.load-data :as ld]
            [witan.datasets :as wds]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;; Historical migration data
(def migration-data (ld/load-datasets {:domestic-in-migrants
                                       "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                       :domestic-out-migrants
                                       "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                       :international-in-migrants
                                       "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                       :international-out-migrants
                                       "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"}))

;; Following 4 datasets are averages of historical migration data calculated in the R model
;; using the equivalent of the first-projection-year-method-average function
(def dom-in-averages (ld/load-dataset :dom-in-averages
                                      "./datasets/test_datasets/r_outputs_for_testing/mig/bristol_proj_domestic_inmigrants_valuesavgfixed.csv"))

(def dom-out-averages (ld/load-dataset :dom-out-averages
                                       "./datasets/test_datasets/r_outputs_for_testing/mig/bristol_proj_domestic_outmigrants_valuesavgfixed.csv"))

(def inter-in-averages (ld/load-dataset :inter-in-averages
                                        "./datasets/test_datasets/r_outputs_for_testing/mig/bristol_proj_international_inmigrants_valuesavgfixed.csv"))

(def inter-out-averages (ld/load-dataset :inter-out-averages
                                         "./datasets/test_datasets/r_outputs_for_testing/mig/bristol_proj_international_outmigrants_valuesavgfixed.csv"))

;; Output of the trends for domestic in-migrants calculated in R
(def trends-domin (ld/load-dataset :dom-in-trends
                                   "./datasets/test_datasets/r_outputs_for_testing/mig/bristol_trends_domestic_inmigrants.csv"))

(deftest first-projection-year-method-average-test
  (testing "The function return the averages on the right year period"
    (let [r-results (:dom-in-averages dom-in-averages)
          clj-results (first-projection-year-method-average (:domestic-in-migrants migration-data)
                                                            :domin :domestic-in 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (wds/subset-ds joined-averages :rows % :cols :domin)
                               (wds/subset-ds joined-averages :rows % :cols :domestic-in) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:dom-out-averages dom-out-averages)
          clj-results (first-projection-year-method-average (:domestic-out-migrants migration-data)
                                                            :domout :domestic-out 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (wds/subset-ds joined-averages :rows % :cols :domout)
                               (wds/subset-ds joined-averages :rows % :cols :domestic-out) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:inter-out-averages inter-out-averages)
          clj-results (first-projection-year-method-average (:international-out-migrants migration-data)
                                                            :intout :international-out 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (wds/subset-ds joined-averages :rows % :cols :intout)
                               (wds/subset-ds joined-averages :rows % :cols :international-out) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:inter-in-averages inter-in-averages)
          clj-results (first-projection-year-method-average (:international-in-migrants migration-data)
                                                            :intin :international-in 2003 2014)
          joined-averages (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (wds/subset-ds joined-averages :rows % :cols :intin)
                               (wds/subset-ds joined-averages :rows % :cols :international-in) 0.0000000001)
                  (range (first (:shape joined-averages))))))))

(deftest first-projection-year-method-trend-test
  (testing "Check the function replicates the one in the R code."
    (let [r-results (:dom-in-trends trends-domin)
          clj-results (first-projection-year-method-trend (:domestic-in-migrants migration-data)
                                                          :domin :domestic-in-clj 2003 2014)
          joined-trends (wds/join r-results clj-results [:gss-code :sex :age])]
      (is (every? #(fp-equals? (wds/subset-ds joined-trends :rows % :cols :domestic-in)
                               (wds/subset-ds joined-trends :rows % :cols :domestic-in-clj) 0.0000001)
                  (range (first (:shape joined-trends))))))))
