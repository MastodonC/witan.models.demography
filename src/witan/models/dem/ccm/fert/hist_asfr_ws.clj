(ns witan.models.dem.ccm.fert.hist-asfr-ws
  (:require [witan.models.dem.ccm.fert.hist-asfr-age :as asfr]
            [witan.workspace-executor.core :as wex]
            [schema.core :as s]
            [witan.models.load-data :as ld]
            [witan.workspace-api :as wapi]))

;; GET DATA FOR ROOT NODES
(def root-data-paths {:births-data "resources/test_data/bristol_births_data.csv"
                      :at-risk-popn "resources/test_data/bristol_denominators.csv"
                      :hist-births-est "resources/test_data/bristol_hist_births_est.csv"})

(def root-data (ld/load-datasets root-data-paths))

(defn get-data
  [keyname]
  (get root-data keyname))

;; SCHEMAS FOR OUTPUT DATSETS
(defn make-ordered-ds-schema [col-vec]
  {:column-names (mapv #(s/one (s/eq (first %)) (str (first %))) col-vec)
   :columns (mapv #(s/one [(second %)] (format "col %s" (name (first %)))) col-vec)
   s/Keyword s/Any})

(def AtRiskThisYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:popn-this-yr s/Num] [:age s/Int]]))

(def AtRiskLastYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn-last-yr s/Num]]))

(def BirthsPoolSchema
  (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year (s/maybe s/Int)] [:gss-code s/Str]
                           [:birth-pool s/Num]]))

;; WORKFLOW
;; Running this workflow is the equivalent of calling asfr/->historic-fertility
;; (wex/view-workflow workflow) will let you see the graph
(def workflow [:get-births-data-year :at-risk-this-birth-year
               :get-births-data-year :at-risk-last-birth-year
               :at-risk-this-birth-year :births-pool
               :at-risk-last-birth-year :births-pool
               ;; :births-pool :births
               ;; :births :fert-rate-without-45-49
               ;; :fert-rate-without-45-49 :fert-rate-with-45-49
               ;; :at-risk-this-fert-last-year :estimated-sya-births-pool
               ;; :at-risk-last-fert-last-year :estimated-sya-births-pool
               ;; :fert-rate-with-45-49 :estimated-sya-births
               ;; :estimated-sya-births-pool :estimated-sya-births
               ;; :estimated-sya-births :estimated-births
               ;; :estimated-sya-births :historic-fertility
               ;; :estimated-births :scaling-factors
               ;; :actual-births :scaling-factors
               ;; :scaling-factors :historic-fertility
               ])

;; CONTRACTS listing terms on which functions operate
(def contracts [{:witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/births-data-year
                 :witan/impl 'witan.models.dem.ccm.fert.hist-asfr-age/->births-data-year
                 :witan/version "1.0"
                 :witan/params-schema nil
                 :witan/inputs [{:witan/schema witan.models.dem.ccm.fert.hist-asfr-age/BirthsDataSchema
                                 :witan/key :births-data
                                 :witan/display-name "Births Data"}]
                 :witan/outputs [{:witan/schema s/Int
                                  :witan/key :yr
                                  :witan/display-name "Births Data Max Year"}]}
                {:witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/at-risk-this-year
                 :witan/impl 'witan.models.dem.ccm.fert.hist-asfr-age/->at-risk-this-year
                 :witan/version "1.0"
                 :witan/params-schema nil
                 :witan/inputs [{:witan/schema witan.models.dem.ccm.fert.hist-asfr-age/AtRiskPopnSchema
                                 :witan/key :at-risk-popn
                                 :witan/display-name "At Risk Population"}
                                {:witan/schema s/Int
                                 :witan/key :yr
                                 :witan/display-name "Year"}]
                 :witan/outputs [{:witan/schema AtRiskThisYearSchema
                                  :witan/key :at-risk-this-year
                                  :witan/display-name "At Risk Population This Year"}]}
                {:witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/at-risk-last-year
                 :witan/impl 'witan.models.dem.ccm.fert.hist-asfr-age/->at-risk-last-year
                 :witan/version "1.0"
                 :witan/params-schema nil
                 :witan/inputs [{:witan/schema witan.models.dem.ccm.fert.hist-asfr-age/AtRiskPopnSchema
                                 :witan/key :at-risk-popn
                                 :witan/display-name "At Risk Population"}
                                {:witan/schema s/Int
                                 :witan/key :yr
                                 :witan/display-name "Year"}]
                 :witan/outputs [{:witan/schema AtRiskLastYearSchema
                                  :witan/key :at-risk-last-year
                                  :witan/display-name "At Risk Population Last Year"}]}
                {:witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/births-pool
                 :witan/impl 'witan.models.dem.ccm.fert.hist-asfr-age/->births-pool
                 :witan/version "1.0"
                 :witan/params-schema nil
                 :witan/inputs [{:witan/schema AtRiskThisYearSchema
                                 :witan/key :at-risk-this-year
                                 :witan/display-name "At Risk Population This Year"}
                                {:witan/schema AtRiskLastYearSchema
                                 :witan/key :at-risk-last-year
                                 :witan/display-name "At Risk Population Last Year"}]
                 :witan/outputs [{:witan/schema BirthsPoolSchema
                                  :witan/key :births-pool
                                  :witan/display-name "Births Pool"}]}])

;; CATALOG linking functions to workflow
(def catalog [{:witan/name :get-births-data-year
               :witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/births-data-year
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-fn
                               'witan.models.dem.ccm.fert.hist-asfr-ws/get-data
                               :witan/input-src-key :births-data
                               :witan/input-dest-key :births-data}]}
              {:witan/name :at-risk-this-birth-year
               :witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/at-risk-this-year
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-fn
                               'witan.models.dem.ccm.fert.hist-asfr-ws/get-data
                               :witan/input-src-key :at-risk-popn
                               :witan/input-dest-key :at-risk-popn}
                              {:witan/input-src-key :yr}]}
              {:witan/name :at-risk-last-birth-year
               :witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/at-risk-last-year
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-fn
                               'witan.models.dem.ccm.fert.hist-asfr-ws/get-data
                               :witan/input-src-key :at-risk-popn
                               :witan/input-dest-key :at-risk-popn}
                              {:witan/input-src-key :yr}]}
              {:witan/name :births-pool
               :witan/fn :demography.ccm.fertility.historic-asfr-by-mother-age/births-pool
               :witan/version "1.0"
               :witan/inputs [{:witan/input-src-key :at-risk-this-year
                               :witan/input-dest-key :at-risk-this-year}
                              {:witan/input-src-key :at-risk-last-year
                               :witan/input-dest-key :at-risk-last-year}]}])

(defn run
  []
  (wex/execute {:workflow workflow
                :catalog catalog
                :contracts contracts}))
