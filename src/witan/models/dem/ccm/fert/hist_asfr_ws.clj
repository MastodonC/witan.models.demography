(ns witan.models.dem.ccm.fert.hist-asfr-ws
  (:require [witan.models.dem.ccm.fert.hist-asfr-age :as asfr]
            [witan.workspace-executor.core :as wex]
            [schema.core :as s]))

;; GET DATA FOR ROOT NODES

;; SCHEMAS

;; WORKFLOW
;; Running this workflow is the equivalent of calling asfr/->historic-fertility
;; (wex/view-workflow workflow) will let you see the graph
(def workflow [:get-births-data-year :at-risk-this-birth-year
               :get-births-data-year :at-risk-next-birth-year
               :at-risk-this-birth-year :births-pool
               :at-risk-next-birth-year :births-pool
               :births-pool :births
               :births :fert-rate-without-45-49
               :fert-rate-without-45-49 :fert-rate-with-45-49
               :at-risk-this-fert-last-year :estimated-sya-births-pool
               :at-risk-last-fert-last-year :estimated-sya-births-pool
               :fert-rate-with-45-49 :estimated-sya-births
               :estimated-sya-births-pool :estimated-sya-births
               :estimated-sya-births :estimated-births
               :estimated-sya-births :historic-fertility    
               :estimated-births :scaling-factors
               :actual-births :scaling-factors
               :scaling-factors :historic-fertility])

;; CATALOG of functions available in workspace
(def catalog [])

;; CONTRACTS linking functions to workflow nodes & defining inputs/outputs
(def contracts [])

(defn run
  []
  ;; (wex/execute {:workflow workflow
  ;;               :catalog catalog
  ;;               :contracts contracts}
  )
