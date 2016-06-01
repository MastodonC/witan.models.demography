(ns witan.models.dem.ccm.components-functions
  (:require [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]))

;; Calculate rates/values for alternative ways to project components of change:
;; 1) Calculate averages for fixed values/rates or trends:
(defn jumpoffyr-method-average
  "Takes in a dataset with historical data, a column name to be averaged,
  a name to rename the average column, a number of years to average on
  and the jumpoff year. Returns a dataset where the column to be averaged contains
  the averages for all the years of interest and is renamed to avg-name."
  [historical-data col-to-avg avg-name number-of-years jumpoff-year]
  (let [last-yr-data (dec jumpoff-year)
        start-year-data (inc (- last-yr-data number-of-years))
        data-of-interest (i/rename-cols {col-to-avg avg-name}
                                        (i/query-dataset historical-data
                                                         {:year {:$gte start-year-data
                                                                 :$lte last-yr-data}}))]
    (wds/rollup data-of-interest :mean avg-name [:gss-code :sex :age])))


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
