(ns dev.analysis
  (:require [incanter.core :as i]
            [witan.phyrexian.ons-ingester :as in]
            [witan.datasets :as wds]
            [witan.models.run-models :refer :all]
            [clojure.core.matrix.dataset :as ds]))

;;;;; Requires witan.phyrexian to be installed locally with `lein install`

(def datasets  {:historic-births-by-age-mother
                "./datasets/default_datasets/fertility/historic_births_by_age_of_mother.csv"
                :historic-births
                "./datasets/default_datasets/fertility/historic_births.csv"
                :historic-population
                "./datasets/default_datasets/core/historic_population.csv"
                :historic-deaths
                "./datasets/default_datasets/mortality/historic_deaths.csv"
                :domestic-in-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_domestic_in.csv"
                :domestic-out-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_domestic_out.csv"
                :international-in-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_international_in.csv"
                :international-out-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_international_out.csv"
                :future-mortality-trend-assumption
                "./datasets/default_datasets/mortality/future_mortality_trend_assumption.csv"
                :future-fertility-trend-assumption
                "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"})

(def gss-bristol "E06000023")

(def params-fixed { ;; Core module
                   :first-proj-year 2015
                   :last-proj-year 2039 ;;final ons year is 2039
                   ;; Fertility module
                   :fert-variant :fixed
                   :fert-base-year 2014
                   :proportion-male-newborns (double (/ 105 205))
                   :fert-scenario :principal-2012
                   ;; Mortality module
                   :mort-variant :average-fixed
                   :mort-scenario :principal
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

(def params-national-trend (assoc params :fert-variant :applynationaltrend :mort-variant :average-applynationaltrend))

(defn compare-projections
  [ons-ds proj-ds]
  (let [comparison (map #(- %1 %2) (nth (:columns ons-ds) 3) (nth (:columns proj-ds) 3))
        comparison-mean (/ (reduce + comparison) (count comparison))
        squared-difference (map #(i/pow (i/abs (- % comparison-mean)) 2) comparison)
        variance (/ (reduce + squared-difference) (count squared-difference))]
    (hash-map :mean-popn-difference comparison-mean
              :variance variance
              :std-dev (i/sqrt variance)
              :max-difference (apply max (map i/abs comparison)))))

(defn ons-ccm-comparison [params]
  (let [proj-bristol (:population (run-workspace datasets gss-bristol params))
        final-year (if (<= (:last-proj-year params) 2039) (:last-proj-year params) 2039)
        ages (sort (distinct (i/$ :age proj-bristol)))
        years (sort (distinct (i/$ :year proj-bristol)))
        years-to-compare (filter (fn [y] (and (>= y (:first-proj-year params)) (<= y final-year))) years)
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
                        (as-> data (sort-by (juxt :gss-code :year :age) data))
                        (ds/dataset))]
    (ds/dataset (reduce (fn [accumulator current-year]
                          (let [ons-current-year (wds/select-from-ds ons-bristol {:year current-year})                   
                                proj-current-year (wds/select-from-ds combine-sex-proj-bristol {:year current-year})                   
                                comparison (compare-projections ons-current-year proj-current-year)]
                            (conj accumulator (assoc comparison :year current-year)))) [] (into [] years-to-compare)))))

(defn make-comparison []
  (let [fixed (ons-ccm-comparison params-fixed)
        national-trend (ons-ccm-comparison params-national-trend)
        _ (write-data-to-csv fixed "fixed_comparison.csv" [:year :mean-popn-difference :max-difference :variance :std-dev])
        _ (write-data-to-csv national-trend "applynationaltrend_comparison.csv" [:year :mean-popn-difference :max-difference :variance :std-dev])]))

