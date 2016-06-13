(ns witan.models.dem.ccm.mort.mortality-mvp-test
  (:require [witan.models.dem.ccm.mort.mortality-mvp :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.core.projection-loop-test :as plt]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;Test inputs
;;historic-deaths = deaths at start of R code
;;historic-births = historic.mort.age0 in R code
;;historic-popn = MYE-Est in R code
(def hist-asmr-inputs (ld/load-datasets
                       {:historic-deaths
                        "resources/test_data/bristol_hist_deaths_mye.csv"
                        :historic-births
                        "resources/test_data/bristol_hist_births_mye.csv"
                        :historic-population
                        "resources/test_data/bristol_hist_popn_mye.csv"}))

;;Output from R calc-historic-asmr function for comparison
(def historic-asmr-r (:historic-asmr (ld/load-dataset
                                      :historic-asmr
                                      "resources/test_data/mortality/bristol_historic_asmr.csv")))

(deftest calc-historic-asmr-test
  (testing "Death rates are calculated correctly."
    (let [hist-asmr-r (ds/rename-columns historic-asmr-r {:death-rate :death-rate-r})
          joined-asmr (-> hist-asmr-inputs
                          calc-historic-asmr
                          :historic-asmr
                          (wds/join hist-asmr-r [:gss-code :district :sex :age :year]))]
      (is (every? #(fp-equals? (i/sel joined-asmr :rows % :cols :death-rate-r)
                               (i/sel joined-asmr :rows % :cols :death-rate)
                               0.0000000001)
                  (range (first (:shape joined-asmr))))))))

(def params {:number-of-years-mort 5 :jumpoff-year-mort 2015})

(deftest project-asmr-test
  (testing "mortality rates projected correctly"
    (let [projected-asmr (-> hist-asmr-inputs
                             calc-historic-asmr
                             (project-asmr params)
                             :initial-projected-mortality-rates)]
      (is (fp-equals? 2.049763E-03
                      (nth (plt/get-popn projected-asmr :proj-death-rate 0 "F") 0)
                      0.000000001))
      (is (fp-equals? 0.2068731
                      (nth (plt/get-popn projected-asmr :proj-death-rate 90 "M") 0)
                      0.0000001)))))
