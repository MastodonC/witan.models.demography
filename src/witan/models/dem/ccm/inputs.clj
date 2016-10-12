(ns witan.models.dem.ccm.inputs
  (:require [witan.workspace-api :refer [definput]]
            [witan.models.dem.ccm.schemas :as s]))

(definput historic-deaths-1-0-0
  {:witan/name :ccm-core-input/historic-deaths
   :witan/version "1.0.0"
   :witan/key :historic-deaths
   :witan/schema s/DeathsSchema})

(definput domestic-in-migrants-1-0-0
  {:witan/name :ccm-core-input/domestic-in-migrants
   :witan/version "1.0.0"
   :witan/key :domestic-in-migrants
   :witan/schema s/DomesticInmigrants})

(definput domestic-out-migrants-1-0-0
  {:witan/name :ccm-core-input/domestic-out-migrants
   :witan/version "1.0.0"
   :witan/key :domestic-out-migrants
   :witan/schema s/DomesticOutmigrants})

(definput international-in-migrants-1-0-0
  {:witan/name :ccm-core-input/international-in-migrants
   :witan/version "1.0.0"
   :witan/key :international-in-migrants
   :witan/schema s/InternationalInmigrants})

(definput international-out-migrants-1-0-0
  {:witan/name :ccm-core-input/international-out-migrants
   :witan/version "1.0.0"
   :witan/key :international-out-migrants
   :witan/schema s/InternationalOutmigrants})

(definput historic-population-1-0-0
  {:witan/name :ccm-core-input/historic-population
   :witan/version "1.0.0"
   :witan/key :historic-population
   :witan/schema s/PopulationSchema})

(definput future-mortality-trend-assumption-1-0-0
  {:witan/name :ccm-core-input/future-mortality-trend-assumption
   :witan/version "1.0.0"
   :witan/key :future-mortality-trend-assumption
   :witan/schema s/NationalTrendsSchema})

(definput future-fertility-trend-assumption-1-0-0
  {:witan/name :ccm-core-input/future-fertility-trend-assumption
   :witan/version "1.0.0"
   :witan/key :future-fertility-trend-assumption
   :witan/schema s/NationalFertilityTrendsSchema})

(definput historic-births-1-0-0
  {:witan/name :ccm-core-input/historic-births
   :witan/version "1.0.0"
   :witan/key :historic-births
   :witan/schema s/BirthsSchema})

(definput historic-births-by-age-mother-1-0-0
  {:witan/name :ccm-core-input/historic-births-by-age-mother
   :witan/version "1.0.0"
   :witan/key :historic-births-by-age-mother
   :witan/schema s/BirthsAgeSexMotherSchema})
