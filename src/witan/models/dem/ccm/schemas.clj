(ns witan.models.dem.ccm.schemas
  (:require [schema.core :as s]
            [witan.models.dem.ccm.models-utils :as m-utils]))

;; Generate schemas
(defn make-ordered-ds-schema [col-vec]
  {:column-names (mapv #(s/one (s/eq (first %)) (str (first %))) col-vec)
   :columns (mapv #(s/one [(second %)] (format "col %s" (name (first %)))) col-vec)
   s/Keyword s/Any})

(defn make-row-schema
  [col-schema]
  (mapv (fn [s] (let [datatype (-> s :schema first)
                      fieldname (:name s)]
                  (s/one datatype fieldname)))
        (:columns col-schema)))

(defn make-col-names-schema
  [col-schema]
  (mapv (fn [s] (let [datatype (:schema s)
                      fieldname (:name s)]
                  (s/one datatype fieldname)))
        (:column-names col-schema)))

;;Define schemas
;;Historic data used in core loop & component modules
(def DomesticInmigrants
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:domin java.lang.Double]]))

(def DomesticOutmigrants
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:domout java.lang.Double]]))

(def InternationalInmigrants
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:intin java.lang.Double]]))

(def InternationalOutmigrants
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:intout java.lang.Double]]))
(def DeathsSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:deaths java.lang.Double]]))

(def BirthsSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:births java.lang.Double]]))

;;For core CCM projection loop using fert/mort/mig inputs from files
(def BirthsBySexSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:births java.lang.Double] ]))

(def DeathsOutputSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:deaths java.lang.Double]]))

(def NetMigrationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:net-mig java.lang.Double]]))

;;For load-data test
(def BirthsDataSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:births java.lang.Double] [:year s/Int]]))

;; For the migration component
(def ProjDomInSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:domestic-in java.lang.Double]]))

(def ProjDomOutSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:domestic-out java.lang.Double]]))

(def ProjInterInSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:international-in java.lang.Double]]))

(def ProjInterOutSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:international-out java.lang.Double]]))

(def DomInAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:domin java.lang.Double]]))

(def DomOutAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:domout java.lang.Double]]))

(def InterInAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:intin java.lang.Double]]))

(def InterOutAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:intout java.lang.Double]]))

;; For the core module
(def PopulationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn java.lang.Double]]))

(def PopulationAtRiskSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn-at-risk java.lang.Double]]))

;; For the mortality component
(def HistASMRSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:death-rate java.lang.Double]]))

(def ProjASMRSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int] [:death-rate java.lang.Double]]))

(def NationalTrendsSchema
  (make-ordered-ds-schema [[:sex s/Str] [:age s/Int] [:year s/Int] [:principal java.lang.Double]
                           [:low java.lang.Double] [:high java.lang.Double]]))

;; For the fertility component
(def HistASFRSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:fert-rate java.lang.Double]]))

(def ProjASFRSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:fert-rate java.lang.Double]]))

(def BirthsAgeSexMotherSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:births java.lang.Double]]))

(def NationalFertilityTrendsSchema
  (make-ordered-ds-schema [[:age s/Int] [:year s/Int] [:principal java.lang.Double]
                           [:low java.lang.Double] [:high java.lang.Double] [:principal-2012 java.lang.Double]
                           [:low-2012 java.lang.Double] [:high-2012 java.lang.Double]]))

(def YearSchema
  (s/constrained s/Int m-utils/year?))
