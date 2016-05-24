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
;;For core CCM projection loop
(def HistBirthsEstSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:var s/Str] [:year s/Int] [:estimate s/Num]]))

(def PopulationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn s/Num]]))

;;For core CCM projection loop using fert/mort/mig inputs from files
(def BirthsBySexSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:births s/Num]]))

(def DeathsSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:death-rate s/Num] [:popn s/Num] [:deaths s/Num]]))

(def NetMigrationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:net-mig s/Num]]))

;;For historic ASFR workflow & input data
(def AtRiskThisYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:popn-this-yr s/Num] [:age s/Int]]))

(def AtRiskLastYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn-last-yr s/Num]]))

(def BirthsPoolSchema
  (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year (s/maybe s/Int)] [:gss-code s/Str]
                           [:birth-pool s/Num]]))

(def BirthsDataSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:births s/Num] [:year s/Int]]))

(def AtRiskPopnSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn s/Num] [:actualyear s/Int] [:actualage s/Int]]))

;; For the migration component
(def MigrationEstimatesSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:var s/Str] [:year s/Int] [:estimate s/Num]]))
