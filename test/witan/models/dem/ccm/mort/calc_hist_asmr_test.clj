(ns witan.models.dem.ccm.mort.calc-hist-asmr-test
  (:require [witan.models.dem.ccm.mort.calc-hist-asmr :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]))

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
    (let [hist-asmr-clj (:historic-asmr (calc-historic-asmr hist-asmr-inputs))
          hist-asmr-r (ds/rename-columns historic-asmr-r {:death-rate :death-rate-r})
          joined-asmr (i/$join [[:gss-code :district :sex :age :year]
                                [:gss-code :district :sex :age :year]]
                               hist-asmr-clj hist-asmr-r)]
         (is (every? #(fp-equals? (i/sel joined-asmr :rows % :cols :death-rate-r)
                                  (i/sel joined-asmr :rows % :cols :death-rate)
                                  0.0000000001)
                     (range (first (:shape joined-asmr))))))))
