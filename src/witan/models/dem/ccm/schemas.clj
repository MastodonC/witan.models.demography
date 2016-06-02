(ns witan.models.dem.ccm.schemas
  (:require [schema.core :as s]))

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
(def ComponentMYESchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:var s/Str] [:year s/Int] [:estimate double]]))

(def PopulationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn double]]))

(def DeathsSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:deaths double]]))

;;For core CCM projection loop using fert/mort/mig inputs from files
(def BirthsBySexSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:births double]]))

(def DeathsOutputSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:death-rate double] [:popn double] [:deaths double]]))

(def NetMigrationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:net-mig double]]))

;;For historic ASFR workflow & input data
(def AtRiskThisYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:popn-this-yr double] [:age s/Int]]))

(def AtRiskLastYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn-last-yr double]]))

(def BirthsPoolSchema
  (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year (s/maybe s/Int)] [:gss-code s/Str]
                           [:birth-pool double]]))

(def BirthsDataSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:births double] [:year s/Int]]))

(def AtRiskPopnSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn double] [:actualyear s/Int] [:actualage s/Int]]))

;; For the migration component
(def ProjDomInSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:domestic-in double]]))

(def ProjDomOutSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:domestic-out double]]))

(def ProjInterInSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:international-in double]]))

(def ProjInterOutSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:international-out double]]))

(def DomInAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:domin double]]))

(def DomOutAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:domout double]]))

(def InterInAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:intin double]]))

(def InterOutAverageSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:intout double]]))

;; For the mortality component
(def HistPopulationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn double]]))

(def BirthsBySexAgeYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:births double]]))

(def HistASMRSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:death-rate double]]))
