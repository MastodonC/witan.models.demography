(ns witan.models.dem.ccm.components-functions
  (:require [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [schema.core :as s]
            [clojure.string :as str]
            [witan.workspace-api.utils :as utils]
            [witan.models.dem.ccm.models-utils :as m-utils]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Calculate jumpoff year rate/value for alternative ways to project components of change: ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;Method 1: Get the value or rate from the final year of histrical data & use this as
;;          the value or rate for the jumpoff year.
(defn jumpoff-year-method-final-year-hist
  "Takes in a dataset with historical values or rates. Returns a dataset that only
   contains the values or rates for the final year of historical data."
  [historical-data col]
  (let [final-year-hist (m-utils/get-last-year historical-data)]
    (-> historical-data
        (wds/select-from-ds {:year final-year-hist})
        (ds/select-columns [:gss-code :sex :age col]))))

;; Method 2: Calculate average value or rate from range of historical data & use this value for
;;           jumpoff year
(defn jumpoff-year-method-average
  "Takes in a dataset with historical data, a column name to be averaged,
  a name for the average column, and years to start and end averaging for.
  Returns a dataset where the column to be averaged contains the averages
  for all the years of interest and is named to avg-name."
  [historical-data col-to-avg avg-name start-year-avg end-year-avg]
  (let [hist-earliest-year (m-utils/get-first-year historical-data)
        hist-latest-year (m-utils/get-last-year historical-data)]
    (-> start-year-avg
        (utils/property-holds?  m-utils/year?
                                (str start-year-avg " is not a year"))
        (utils/property-holds?  #(>= % hist-earliest-year)
                                "Start year must be more than or equal to earliest year in dataset"))
    (-> end-year-avg
        (utils/property-holds?  m-utils/year?
                                (str end-year-avg " is not a year"))
        (utils/property-holds?  #(<= % hist-latest-year)
                                "End year must be less than or equal to the latest year in the dataset"))
    (-> (wds/select-from-ds historical-data
                            {:year {:$gte start-year-avg
                                    :$lte end-year-avg}})
        (ds/rename-columns {col-to-avg avg-name})
        (wds/rollup :mean avg-name [:gss-code :sex :age]))))

;;Method 3: Calculate trend (perform linear regression) over range of histrical data &
;;          using this trend get the value or rate to use for the jumpoff year.
(defn jumpoff-year-method-trend
  "Takes in a dataset with historical data, a column name for a trend to be
  calculated for, a name for the trend col and years to start and end to
  calculate the trend for. Returns a dataset with a new col for the calculated trend"
  [historical-data trend-col trend-out start-year-avg end-year-avg]
  (let [hist-earliest-year (m-utils/get-first-year historical-data)
        hist-latest-year (m-utils/get-last-year historical-data)
        _ (-> start-year-avg
              (utils/property-holds?  m-utils/year?
                                      (str start-year-avg " is not a year"))
              (utils/property-holds?  #(>= % hist-earliest-year)
                                      "Start year must be more than or equal to earliest year in dataset"))
        _ (-> end-year-avg
              (utils/property-holds?  m-utils/year?
                                      (str end-year-avg " is not a year"))
              (utils/property-holds?  #(<= % hist-latest-year)
                                      "End year must be less than or equal to the latest year in the dataset"))
        grouped-data (-> historical-data
                         (wds/select-from-ds {:year {:$gte start-year-avg
                                                     :$lte end-year-avg}})
                         (wds/group-ds [:sex :gss-code :age]))
        lm (map (fn [[k v]]
                  (:coefs (wds/linear-model (wds/subset-ds v :cols trend-col)
                                            (wds/subset-ds v :cols :year))))
                grouped-data)
        assoc-coefs (mapv (fn [[k v] i]
                            (assoc k
                                   :intercept (first i)
                                   :regres-coef (second i)))
                          grouped-data lm)]
    (-> assoc-coefs
        ds/dataset
        (wds/add-derived-column trend-out
                                [:regres-coef :intercept]
                                (fn [y i] (let [t (+ i (* y (inc end-year-avg)))]
                                            (if (neg? t)
                                              0.0
                                              t))))
        (ds/select-columns [:gss-code :sex :age trend-out]))))

(defn add-years-to-fixed-methods
  "Plots fixed rates across all projected years"
  [fixed-projection first-proj-year last-proj-year]
  (let [n (wds/row-count fixed-projection)]
    (->> (range first-proj-year (inc last-proj-year))
         (map #(ds/add-column fixed-projection :year (repeat n %)))
         (reduce ds/join-rows))))

;; Project component for fixed rates:
(defn project-component-fixed-rates
  "Takes in a population at risk and fixed rates for a component.
  Outputs the result of applying the rates to that component."
  [population-at-risk fixed-rates col-fixed-rates col-result]
  (-> fixed-rates
      (wds/join population-at-risk [:gss-code :age :sex])
      (wds/add-derived-column col-result [col-fixed-rates :popn-at-risk] *)
      (ds/select-columns [:gss-code :sex :age :year col-result])))

(defn order-ds
  [dataset col-key]
  (utils/property-holds? dataset ds/dataset? "Not a dataset")
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

(defn lag
  "Data must be ordered chronologically in order to work correctly"
  [dataset col-key]
  (-> dataset
      (ds/to-map)
      (col-key)
      (#(cons 0 %))
      (butlast)))

(defn project-component
  [population-at-risk rates loop-year col-rates col-result]
  (-> rates
      (wds/select-from-ds {:year loop-year})
      (wds/join population-at-risk [:gss-code :age :sex])
      (wds/add-derived-column col-result [col-rates :popn-at-risk] *)
      (ds/select-columns [:gss-code :sex :age :year col-result])))
