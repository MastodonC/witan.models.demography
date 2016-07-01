(ns witan.models.dem.ccm.components-functions
  (:require [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]))

;; Calculate rates/values for alternative ways to project components of change:

;; Calculate averages for fixed values/rates or trends:
(defn jumpoffyr-method-average
  "Takes in a dataset with historical data, a column name to be averaged,
  a name to rename the average column, a number of years to average on
  and the jumpoff year. Returns a dataset where the column to be averaged contains
  the averages for all the years of interest and is renamed to avg-name."
  [historical-data col-to-avg avg-name start-yr-avg end-yr-avg]
  ;; start-yr-range should be >= than the earliest year in historical-data
  ;; end-yr-range should be <= than the latest year in historical-data
  (-> (i/query-dataset historical-data
                       {:year {:$gte start-yr-avg
                               :$lte end-yr-avg}})
      (ds/rename-columns {col-to-avg avg-name})
      (wds/rollup :mean avg-name [:gss-code :sex :age])))

;; Project component for fixed rates:
(defn project-component-fixed-rates
  "Takes in a population at risk and fixed rates for a component.
  Outputs the result of applying the rates to that component."
  [population-at-risk fixed-rates col-fixed-rates col-result]
  (-> fixed-rates
      (wds/join population-at-risk [:gss-code :age :sex])
      (wds/add-derived-column col-result [col-fixed-rates :popn] *)
      (ds/select-columns [:gss-code :sex :age :year col-result])))

(defn order-ds
  [dataset col-key]
  (cond (keyword? col-key) (->> dataset
                                ds/row-maps
                                vec
                                (sort-by col-key)
                                ds/dataset)
        (vector? col-key) (->> dataset
                               ds/row-maps
                               vec
                               (sort-by (apply juxt col-key))
                               ds/dataset)))
