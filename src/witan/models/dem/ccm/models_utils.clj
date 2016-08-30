(ns witan.models.dem.ccm.models-utils
  (:require [clojure.set]
            [witan.workspace-api :refer [defworkflowfn
                                         defworkflowoutput
                                         defworkflowinput]]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [witan.workspace-api.utils :as utils]))

(defn year? [n] (and (>= n 1900) (<= n 2100)))

(defn year-column-exists?
  [dataset]
  (contains? (set (:column-names dataset)) :year)) 

(defn get-first-year
  [dataset]
  (utils/property-holds?  dataset year-column-exists?
                          (str "Dataset must have a year column"))
  (reduce min (ds/column dataset :year)))

(defn get-last-year
  [dataset]
  (utils/property-holds?  dataset year-column-exists?
                          (str "Dataset must have a year column"))
  (reduce max (ds/column dataset :year)))

(defn get-meta
  [v]
  (-> v :var meta :witan/metadata))

(defn make-contracts
  [task-coll]
  (distinct (mapv (fn [[k v]] (get-meta v)) task-coll)))

(defn make-catalog
  [task-coll]
  (mapv (fn [[k v]]
          (let [m (hash-map :witan/name k
                            :witan/version "1.0.0"
                            :witan/type (:witan/type (get-meta v))
                            :witan/fn (:witan/name (get-meta v)))]
            (if (:params v)
              (assoc m :witan/params (:params v))
              m))) task-coll))

(defworkflowfn fn-out
  {:witan/name :workspace-test/fn-out
   :witan/version "1.0.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:finished? (s/eq true)}}
  [data _]
  ;; This is where data is persisted to a file, S3 etc.
  {:finished? true})

(defworkflowoutput out
  {:witan/name :workspace-test/out
   :witan/version "1.0.0"
   :witan/input-schema {:* s/Any}}
  [data _]
  ;; This is where data is persisted to a file, S3 etc.
  {:finished? true})
