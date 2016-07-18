(ns witan.models.dem.ccm.components-functions
  (:require [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [schema.core :as s]
            [incanter.stats :as st]
            [clojure.string :as str]))

;; Calculate rates/values for alternative ways to project components of change:

;; Calculate averages for fixed values/rates or trends:
(defn jumpoffyr-method-average
  "Takes in a dataset with historical data, a column name to be averaged,
  a name for the average column, and years to start and end averaging for.
  Returns a dataset where the column to be averaged contains the averages
  for all the years of interest and is named to avg-name."
  [historical-data col-to-avg avg-name start-yr-avg end-yr-avg]
  (let [hist-earliest-yr (reduce min (ds/column historical-data :year))
        hist-latest-yr (reduce max (ds/column historical-data :year))
        start-yr (s/validate (s/pred #(>= % hist-earliest-yr)) start-yr-avg)
        end-yr (s/validate (s/pred #(<= % hist-latest-yr)) end-yr-avg)]
    (-> (i/query-dataset historical-data
                         {:year {:$gte start-yr
                                 :$lte end-yr}})
        (ds/rename-columns {col-to-avg avg-name})
        (wds/rollup :mean avg-name [:gss-code :sex :age]))))

(defn jumpoffyr-method-trend
  "Takes in a dataset with historical data, a column name for a trend to be
  calculated for, a name for the trend col and years to start and end to
  calculate the trend for. Returns a dataset with a new col for the calculated trend"
  [historical-data trend-col trend-out start-yr-avg end-yr-avg]
  (let [hist-earliest-yr (reduce min (ds/column historical-data :year))
        hist-latest-yr (reduce max (ds/column historical-data :year))
        start-yr (s/validate (s/pred #(>= % hist-earliest-yr)) start-yr-avg)
        end-yr (s/validate (s/pred #(<= % hist-latest-yr)) end-yr-avg)
        grouped-data (->> historical-data
                          (#(i/query-dataset % {:year {:$gte start-yr
                                                       :$lte end-yr}}))
                          (i/$group-by [:sex :gss-code :age]))
        lm (map (fn [[k v]]
                  (:coefs (st/linear-model (i/$ trend-col v) (i/$ :year v))))
                grouped-data)
        newest-data (mapv (fn [[k v] i]
                            (assoc k
                                   :intercept (first i)
                                   :regres-coef (second i)))
                          grouped-data lm)]
    (-> newest-data
        ds/dataset
        (wds/add-derived-column trend-out
                                [:regres-coef :intercept]
                                (fn [y i] (let [t (+ i (* y (inc end-yr)))]
                                            (if (neg? t)
                                              0
                                              t))))
        (ds/select-columns [:gss-code :sex :age trend-out]))))

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
