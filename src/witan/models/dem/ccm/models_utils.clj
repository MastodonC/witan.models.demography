(ns witan.models.dem.ccm.models-utils
  (:require [witan.workspace-api :refer [defworkflowfn]]
            [witan.models.load-data :as ld]
            [schema.core :as s]))

(defn get-meta
  [v]
  (let [meta-key (if (:pred? v) :witan/workflowpred :witan/workflowfn)]
    (-> v :var meta meta-key)))

(defn make-contracts
  [task-coll]
  (distinct (mapv (fn [[k v]] (get-meta v)) task-coll)))

(defn make-catalog
  [task-coll]
  (mapv (fn [[k v]]
          (hash-map :witan/name k
                    :witan/version "1.0"
                    :witan/params (:params v)
                    :witan/fn (:witan/name (get-meta v)))) task-coll))

(defworkflowfn resource-csv-loader
  "Loads CSV files from resources"
  {:witan/name :workspace-test/resource-csv-loader
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:* s/Any}
   :witan/param-schema {:src s/Str
                        :key s/Keyword}
   :witan/exported? false}
  [_ {:keys [src key]}]
  (ld/load-dataset key src))

(defworkflowfn out
  "Print a message to indicate we were called"
  {:witan/name :workspace-test/out
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:finished? (s/pred true?)}
   :witan/exported? true}
  [data _]
  ;; This is where data is persisted to a file, S3 etc.
  {:finished? true})
