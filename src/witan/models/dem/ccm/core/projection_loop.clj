(ns witan.models.dem.ccm.core.projection-loop
  (:require [clojure.string :as str]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.workspace-api :refer [defworkflowfn merge-> rename-keys]]))

;; Schemas for data inputs/ouputs:
(defn make-ordered-ds-schema [col-vec]
  {:column-names (mapv #(s/one (s/eq (first %)) (str (first %))) col-vec)
   :columns (mapv #(s/one [(second %)] (format "col %s" (name (first %)))) col-vec)
   s/Keyword s/Any})

(def HistoricPopnEstimates
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn s/Int]]))

(def StartingPopn
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn s/Int]]))

;; Functions:
(defworkflowfn ->starting-popn
  "Takes in a dataset of popn estimates and a year for the projection.
   Returns a dataset w/ the starting popn (popn for the previous year)."
  {:witan/name :get-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:historic-popn-estimates HistoricPopnEstimates}
   :witan/param-schema {:year s/Int}
   :witan/output-schema {:starting-popn StartingPopn}}
  [{:keys [historic-popn-estimates]} {:keys [year]}]
  {:starting-popn (i/query-dataset historic-popn-estimates {:year (dec year)})})

(defworkflowfn core-loop
  "Takes in a dataset of popn estimates, a first year
   and last year. Implement the looping to output the projections
   for the range of years between the first and last year."
  {:witan/name :exec-core-loop
   :witan/version "1.0"
   :witan/input-schema {:historic-popn-estimates HistoricPopnEstimates}
   :witan/param-schema {:first-proj-year s/Int
                        :last-proj-year s/Int}
   :witan/output-schema {:starting-popn StartingPopn} ;; TO BE UPDATED
   :witan/exported? true}
  [inputs params]
  (let [new-params (rename-keys params {:first-proj-year :year})]
    (->starting-popn inputs new-params)))
