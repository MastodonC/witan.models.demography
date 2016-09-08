(ns witan.models.dem.ccm.inputs
  (:require [witan.workspace-api :refer [definput]]
            [witan.models.dem.ccm.schemas :as s]))

(definput in-hist-deaths-by-age-and-sex-1-0-0
  {:witan/name :ccm-core-input/in-hist-deaths-by-age-and-sex
   :witan/version "1.0.0"
   :witan/key :historic-deaths
   :witan/schema s/DeathsSchema})

(definput in-hist-dom-in-migrants-1-0-0
  {:witan/name :ccm-core-input/in-hist-dom-in-migrants
   :witan/version "1.0.0"
   :witan/key :domestic-in-migrants
   :witan/schema s/DomesticInmigrants})

(definput in-hist-dom-out-migrants-1-0-0
  {:witan/name :ccm-core-input/in-hist-dom-out-migrants
   :witan/version "1.0.0"
   :witan/key :domestic-out-migrants
   :witan/schema s/DomesticOutmigrants})

(definput in-hist-intl-in-migrants-1-0-0
  {:witan/name :ccm-core-input/in-hist-intl-in-migrants
   :witan/version "1.0.0"
   :witan/key :international-in-migrants
   :witan/schema s/InternationalInmigrants})

(definput in-hist-intl-out-migrants-1-0-0
  {:witan/name :ccm-core-input/in-hist-intl-out-migrants
   :witan/version "1.0.0"
   :witan/key :international-out-migrants
   :witan/schema s/InternationalOutmigrants})

(definput in-hist-popn-1-0-0
  {:witan/name :ccm-core-input/in-hist-popn
   :witan/version "1.0.0"
   :witan/key :historic-population
   :witan/schema s/PopulationSchema})

(definput in-future-mort-trend-1-0-0
  {:witan/name :ccm-core-input/in-future-mort-trend
   :witan/version "1.0.0"
   :witan/key :future-mortality-trend-assumption
   :witan/schema s/NationalTrendsSchema})

(definput in-future-fert-trend-1-0-0
  {:witan/name :ccm-core-input/in-future-fert-trend
   :witan/version "1.0.0"
   :witan/key :future-fertility-trend-assumption
   :witan/schema s/NationalFertilityTrendsSchema})

(definput in-hist-total-births-1-0-0
  {:witan/name :ccm-core-input/in-hist-total-births
   :witan/version "1.0.0"
   :witan/key :historic-births
   :witan/schema s/BirthsSchema})

(definput in-proj-births-by-age-of-mother-1-0-0
  {:witan/name :ccm-core-input/in-proj-births-by-age-of-mother
   :witan/version "1.0.0"
   :witan/key :historic-births-by-age-mother
   :witan/schema s/BirthsAgeSexMotherSchema})
