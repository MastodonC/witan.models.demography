(ns witan.models.dem.ccm.components-functions-test
  (:require [witan.models.dem.ccm.components-functions :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(def domestic-mig-in (ld/load-dataset :migration-mye
                                      "resources/test_data/bristol_dom_in_mig.csv"))

(def dom-in-averages (ld/load-dataset :dom-in-averages
                                      "resources/test_data/bristol_dom_in_avg.csv"))

(deftest calculate-averages-test
  (testing "The function return the averages on the right year period"
    (let [r-results (:dom-in-averages dom-in-averages)
          clj-results (calculate-averages (:migration-mye domestic-mig-in) 12 2015)
          joined-averages (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                                 r-results clj-results)]
      (is (every? #(fp-equals? (i/sel joined-averages :rows % :cols :domin)
                               (i/sel joined-averages :rows % :cols :estimate) 0.0001)
                  (range (first (:shape joined-averages))))))))
