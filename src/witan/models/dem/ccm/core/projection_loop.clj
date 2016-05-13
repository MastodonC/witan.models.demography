(ns witan.models.dem.ccm.core.projection-loop
  (:require [clojure.string :as str]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.workspace-api :refer [defworkflowfn merge-> rename-keys]]
            [witan.datasets :as wds]))

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

(defworkflowfn keep-looping?
  {:witan/name :ccm-core/ccm-loop-pred
   :witan/version "1.0"
   :witan/input-schema {:population PopulationSchema}
   :witan/param-schema {:last-proj-year s/Int}
   :witan/output-schema {:loop-predicate s/Bool}
   ;; :witan/predicate? true
   :witan/exported? true}
  [{:keys [population]} {:keys [last-proj-year]}]
  {:loop-predicate (< (get-last-yr-from-popn population) last-proj-year)})

(defworkflowfn select-starting-popn
  "Takes in a dataset of popn estimates.
   Returns a dataset where the last year's population from the input data
   is appended as the starting population for the next year's projection"
  {:witan/name :ccm-core/get-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:population PopulationSchema}
   :witan/param-schema {:_ nil}
   :witan/output-schema {:population PopulationSchema}}
  [{:keys [population]} _]
  (let [latest-yr (get-last-yr-from-popn population)
        last-yr-data (i/query-dataset population {:year latest-yr})
        update-yr (i/replace-column :year (i/$map inc :year last-yr-data) last-yr-data)]
    {:population (ds/join-rows population update-yr)}))

(defworkflowfn age-on
  "Takes in a dataset of popn estimates.
   Returns a dataset where the last year's population is aged on 1 year.
   91 year olds are added to the 90 year olds in the aged-on popn."
  {:witan/name :ccm-cor/age-on
   :witan/version "1.0"
   :witan/input-schema {:population PopulationSchema}
   :witan/param-schema {:_ nil}
   :witan/output-schema {:population PopulationSchema}}
  [{:keys [population]} _]
  (let [latest-yr (get-last-yr-from-popn population)
        last-yr-data (i/query-dataset population {:year latest-yr})
        prev-yrs-data (i/query-dataset population {:year {:$ne latest-yr}})
        aged-on (i/replace-column :age (i/$map
                                        (fn [v] (if (< v 90) (inc v) v))
                                        :age last-yr-data) last-yr-data)
        grouped (wds/rollup :sum :popn [:gss-code :sex :age :year] aged-on)]
    {:population (ds/join-rows prev-yrs-data grouped)}))

(defn looping-test
  [inputs params]
  (loop [inputs inputs]
    (let [inputs' (->> (select-starting-popn inputs)
                       (age-on))]
      (println (format "Projecting for year %d..." (get-last-yr-from-popn (:population inputs'))))
      (if (:loop-predicate (keep-looping? inputs' params))
        (recur inputs')
        (:population inputs')))))
