(ns witan.models.dem.ccm.components-functions
  (:require [incanter.core :as i]
            [clojure.core.matrix.dataset :as ds]))

;; Calculate rates/values for alternative ways to project components of change:
;; 1) Calculate averages for fixed values/rates or trends:
(defn calculate-averages
  "Takes in a dataset with historical data, a number of years to average on
  and the jumpoff year. Returns a datasets with a new column containing the
  averages for a specific component for all the years of interest."
  [historical-data nb-of-years jumpoff-year]
  (let [last-yr-data (dec jumpoff-year)
        start-year (- last-yr-data nb-of-years)
        data-of-interest (i/query-dataset historical-data
                                          {:year {:$gte start-year :$lte last-yr-data}})]
    ))
