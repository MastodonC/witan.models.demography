(ns witan.models.dem.ccm.components-functions-test
  (:require [witan.models.dem.ccm.components-functions :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(def domestic-mig-in (ld/load-dataset :migration-mye
                                      "resources/test_data/migration/bristol_dom_in_mig.csv"))

(def dom-in-averages (ld/load-dataset :dom-in-averages
                                      "resources/test_data/migration/bristol_dom_in_avg.csv"))

(def dom-out-averages (ld/load-dataset :dom-out-averages
                                      "resources/test_data/migration/bristol_dom_out_avg.csv"))

(def inter-in-averages (ld/load-dataset :inter-in-averages
                                      "resources/test_data/migration/bristol_inter_in_avg.csv"))

(def inter-out-averages (ld/load-dataset :inter-out-averages
                                      "resources/test_data/migration/bristol_inter_out_avg.csv"))

(deftest calculate-averages-test
  (testing "The function return the averages on the right year period"
    (let [r-results (:dom-in-averages dom-in-averages)
          clj-results (calculate-averages (:migration-mye domestic-mig-in)
                                          :estimate :domestic-in 12 2015)
          joined-averages (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                                   r-results clj-results)]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :domin)
                               (i/sel joined-averages :rows % :cols :domestic-in) 0.0001)
                  (range (first (:shape joined-averages))))))))
