(ns witan.models.dem.ccm.components-functions-test
  (:require [witan.models.dem.ccm.components-functions :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;Historical migration data
(def migration-data (ld/load-datasets {:domestic-in-migrants
                                       "resources/test_data/migration/bristol_dom_in_mig.csv"
                                       :domestic-out-migrants
                                       "resources/test_data/migration/bristol_dom_out_mig.csv"
                                       :international-in-migrants
                                       "resources/test_data/migration/bristol_inter_in_mig.csv"
                                       :international-out-migrants
                                       "resources/test_data/migration/bristol_inter_out_mig.csv"}))

;;Following 4 datasets are averages of historical migration data calculated in the R model
;;using the equivalent of the jumpoffyr-method-average function
(def dom-in-averages (ld/load-dataset :dom-in-averages
                                      "resources/test_data/migration/bristol_dom_in_avg.csv"))

(def dom-out-averages (ld/load-dataset :dom-out-averages
                                       "resources/test_data/migration/bristol_dom_out_avg.csv"))

(def inter-in-averages (ld/load-dataset :inter-in-averages
                                        "resources/test_data/migration/bristol_inter_in_avg.csv"))

(def inter-out-averages (ld/load-dataset :inter-out-averages
                                         "resources/test_data/migration/bristol_inter_out_avg.csv"))

(deftest jumpoffyr-method-average-test
  (testing "The function return the averages on the right year period"
    (let [r-results (:dom-in-averages dom-in-averages)
          clj-results (jumpoffyr-method-average (:domestic-in-migrants migration-data)
                                          :estimate :domestic-in 12 2015)
          joined-averages (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                                   r-results clj-results)]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :domin)
                               (i/sel joined-averages :rows % :cols :domestic-in) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:dom-out-averages dom-out-averages)
          clj-results (jumpoffyr-method-average (:domestic-out-migrants migration-data)
                                          :estimate :domestic-out 12 2015)
          joined-averages (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                                   r-results clj-results)]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :domout)
                               (i/sel joined-averages :rows % :cols :domestic-out) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:inter-out-averages inter-out-averages)
          clj-results (jumpoffyr-method-average (:international-out-migrants migration-data)
                                          :estimate :international-out 12 2015)
          joined-averages (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                                   r-results clj-results)]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :intout)
                               (i/sel joined-averages :rows % :cols :international-out) 0.0000000001)
                  (range (first (:shape joined-averages))))))
    (let [r-results (:inter-in-averages inter-in-averages)
          clj-results (jumpoffyr-method-average (:international-in-migrants migration-data)
                                          :estimate :international-in 12 2015)
          joined-averages (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                                   r-results clj-results)]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :intin)
                               (i/sel joined-averages :rows % :cols :international-in) 0.0000000001)
                  (range (first (:shape joined-averages))))))))
