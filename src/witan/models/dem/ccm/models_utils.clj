(ns witan.models.dem.ccm.models-utils
  (:require [clojure.set]
            [witan.workspace-api :refer [defworkflowfn
                                         defworkflowoutput
                                         defworkflowinput]]
            [witan.models.load-data :as ld]
            [schema.core :as s]))

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

(defworkflowinput resource-csv-loader
  "Loads CSV files from resources"
  {:witan/name :workspace-test/resource-csv-loader
   :witan/version "1.0.0"
   :witan/output-schema {:* s/Any}
   :witan/param-schema {:src s/Str
                        :key s/Keyword}}
  [_ {:keys [src key]}]
  (ld/load-dataset key src))

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
