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

(def PopulationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn s/Int]]))

;; Functions:
(defn get-last-yr-from-popn
  "Takes in a dataset. Select the latest year and returns it.
  Note: expect a :year column."
  [popn]
  (reduce max (i/$ :year popn)))

(defworkflowfn select-starting-popn
  "Takes in a dataset of popn estimates and a year for the projection.
   Returns a dataset w/ the starting popn (popn for the previous year)."
  {:witan/name :ccm-core/get-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:population PopulationSchema}
   :witan/param-schema {:first-proj-year s/Int}
   :witan/output-schema {:population PopulationSchema}}
  [{:keys [population]} {:keys [first-proj-year]}]
  (let [latest-yr (get-last-yr-from-popn population)
        last-yr-data (i/query-dataset population {:year latest-yr})
        update-yr (i/replace-column :year (i/$map inc :year last-yr-data) last-yr-data)]
    {:population (i/conj-rows population update-yr)}))
