(ns dev.analysis
  (:require [incanter.core :as i]
            [witan.phyrexian.ons-ingester :as in]
            [witan.datasets :as wds]
            [witan.models.run-models :refer :all]
            [clojure.core.matrix.dataset :as ds]))

;;;;; Requires witan.phyrexian to be installed locally with `lein install`

(def datasets  {:historic-births-by-age-mother
                "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                :historic-births
                "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"
                :historic-population
                "./datasets/test_datasets/model_inputs/core/bristol_hist_popn_est.csv"
                :historic-deaths
                "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"
                :domestic-in-migrants
                "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                :domestic-out-migrants
                "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                :international-in-migrants
                "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                :international-out-migrants
                "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                :future-mortality-trend-assumption
                "./datasets/test_datasets/model_inputs/mort/future_mortality_trend_assumption.csv"})

(def gss-bristol "E06000023")

(def params-2015 {;; Core module
                  :first-proj-year 2015
                  :last-proj-year 2016
                  ;; Fertility module
                  :fert-base-year 2014
                  :proportion-male-newborns (double (/ 105 205))
                  ;; Mortality module
                  :start-year-avg-mort 2010
                  :end-year-avg-mort 2014
                  ;; Migration module
                  :start-year-avg-domin-mig 2003
                  :end-year-avg-domin-mig 2014
                  :start-year-avg-domout-mig 2003
                  :end-year-avg-domout-mig 2014
                  :start-year-avg-intin-mig 2003
                  :end-year-avg-intin-mig 2014
                  :start-year-avg-intout-mig 2003
                  :end-year-avg-intout-mig 2014})

(defn ons-ccm-comparison []
  (let [proj-bristol (run-workspace datasets gss-bristol params-2015)
        proj-bristol-2015 (wds/select-from-ds proj-bristol {:year 2015})
        ages (sort (distinct (i/$ :age proj-bristol-2015)))
        years (sort (distinct (i/$ :year proj-bristol)))
        combine-sex-proj-bristol-2015 (ds/dataset [:gss.code :age :year :popn]
                                                  (mapv (fn [age]
                                                          (vec [gss-bristol
                                                                age
                                                                2015
                                                                (reduce + (i/$ :popn (i/query-dataset proj-bristol-2015
                                                                                                      {:age age})))]))
                                                        ages))
        combine-sex-proj-bristol (ds/dataset [:gss.code :age :year :popn]
                                             (into [] (mapcat (fn [year]
                                                                (mapv (fn [age]
                                                                        (vec [gss-bristol
                                                                              age
                                                                              year
                                                                              (reduce + (i/$ :popn (-> (i/query-dataset proj-bristol
                                                                                                                        {:year {:eq year}})
                                                                                                       (i/query-dataset {:age age}))))]))
                                                                      ages))) years))
        ons-bristol (-> (in/process-ons-data "datasets/test_datasets/bristol_2014_snpp_population_persons.csv" in/onsSchema)
                        ds/dataset)
        ons-bristol-2015 (wds/select-from-ds ons-bristol {:year 2015})
        comparison (map #(- %1 %2) (nth (:columns ons-bristol-2015) 3) (nth (:columns combine-sex-proj-bristol-2015) 3))
        comparison-mean (/ (reduce + comparison) (count comparison))
        squared-difference (map #(i/pow (i/abs (- % comparison-mean)) 2) comparison)
        variance (/ (reduce + squared-difference) (count squared-difference))]
    (hash-map :variance variance
              :std-dev (i/sqrt variance)
              :max-difference (apply max (map i/abs comparison)))))
