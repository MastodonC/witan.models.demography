(ns witan.models.workspace-test
  (:require [clojure.test :refer :all]
            [schema.test :as st]
            [schema.core :as s]
            [clojure.core.async :refer [pipe <!! put! close!]]
            [clojure.core.async.lab :refer [spool]]
            [witan.workspace.onyx :as o]
            [witan.models.functions :as fc]
            [onyx
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin
             [redis]
             [core-async :refer [get-core-async-channels]]]
            [onyx.tasks
             [core-async :as core-async]
             [redis :as redis]]
            [witan.models.config :refer [config]]
            [witan.models.load-data :refer [load-dataset]]
            ;;
            [witan.models.dem.ccm.fert.fertility-mvp]
            [witan.models.dem.ccm.mort.mortality-mvp]
            [witan.models.dem.ccm.mig.net-migration]
            [witan.models.dem.ccm.core.projection-loop]))

(defn add-source-and-sink
  [job]
  (-> job
      (add-task (core-async/input :in (:batch-settings config)))
      (add-task (core-async/output :out (:batch-settings config)))))

(defn run-job
  ([job data]
   (run-job job data 7))
  ([job data n]
   (let [{:keys [env-config
                 peer-config]} config
         {:keys [out in]} (get-core-async-channels job)]
     (with-test-env [test-env [n env-config peer-config]]
       (pipe (spool [data :done]) in)
       (onyx.test-helper/validate-enough-peers! test-env job)
       (let [job-id (:job-id (onyx.api/submit-job peer-config job))
             result (<!! out)]
         (onyx.api/await-job-completion peer-config job-id)
         result)))))

;; Execute before task
#_(defn log-before-batch
    [event lifecycle]
    (println "Executing before batch" (:lifecycle/task lifecycle))
    {})

#_(def log-calls
    {:lifecycle/before-batch log-before-batch})

#_(deftest loop+merge-workspace-executed-on-onyx
    (let [state {:test "blah" :number 2}
          onyx-job (add-source-and-sink
                    (o/workspace->onyx-job
                     {:workflow [[:in   :inc]
                                 [:in   :mul2]
                                 [:inc  :dupe]
                                 [:dupe [:enough? :mulX :inc]]
                                 [:mul2 :dupe2]
                                 [:dupe2 :merge]
                                 [:mulX :merge]
                                 [:merge :out]]
                      :catalog [{:witan/name :inc
                                 :witan/fn :witan.models.functions/my-inc}
                                {:witan/name :mul2
                                 :witan/fn :witan.models.functions/mul2}
                                {:witan/name :dupe
                                 :witan/fn :witan.models.functions/dupe
                                 :witan/params {:from :number
                                                :to :foo}}
                                {:witan/name :dupe2
                                 :witan/fn :witan.models.functions/dupe
                                 :witan/params {:from :number
                                                :to :foo2}}
                                {:witan/name :mulX
                                 :witan/fn :witan.models.functions/mulX
                                 :witan/params {:x 4}}
                                {:witan/name :merge
                                 :witan/fn :clojure.core/identity}
                                {:witan/name :enough?
                                 :witan/fn :witan.models.functions/gte-ten}]}
                     config))
          onyx-job' onyx-job
          #_(update onyx-job :lifecycles concat [{:lifecycle/task :inc
                                                  :lifecycle/calls :witan.workspace.acceptance.onyx-test/log-calls}
                                                 {:lifecycle/task :dupe
                                                  :lifecycle/calls :witan.workspace.acceptance.onyx-test/log-calls}
                                                 {:lifecycle/task :mulX
                                                  :lifecycle/calls :witan.workspace.acceptance.onyx-test/log-calls}])
          result (run-job onyx-job' state 13)
          prediction {:test "blah", :number 40, :foo2 4, :foo 10}]
      (is (= result prediction))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def tasks
  {:proj-historic-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/project-asfr-finalyrhist-fixed
                                :params {:fert-last-yr 2014
                                         :start-yr-avg-fert 2014
                                         :end-yr-avg-fert 2014}}
   :join-popn-latest-yr        {:var #'witan.models.dem.ccm.core.projection-loop/join-popn-latest-yr}
   :add-births                 {:var #'witan.models.dem.ccm.core.projection-loop/add-births}
   :project-deaths             {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-deaths-from-fixed-rates}
   :proj-domestic-in-migrants  {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-in-migrants
                                :params {:start-yr-avg-dom-mig 2003
                                         :end-yr-avg-dom-mig 2014}}
   :calc-historic-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/calc-historic-asmr}
   :proj-domestic-out-migrants {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-out-migrants
                                :params {:start-yr-avg-dom-mig 2003
                                         :end-yr-avg-dom-mig 2014}}
   :remove-deaths              {:var #'witan.models.dem.ccm.core.projection-loop/remove-deaths}
   :age-on                     {:var #'witan.models.dem.ccm.core.projection-loop/age-on}
   :project-births             {:var #'witan.models.dem.ccm.fert.fertility-mvp/births-projection
                                :params {:proportion-male-newborns (double (/ 105 205))}}
   :proj-historic-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-asmr
                                :params {:start-yr-avg-mort 2010
                                         :end-yr-avg-mort 2014}}
   :select-starting-popn       {:var #'witan.models.dem.ccm.core.projection-loop/select-starting-popn}
   :calc-historic-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/calculate-historic-asfr
                                :params {:fert-last-yr 2014}}
   :apply-migration            {:var #'witan.models.dem.ccm.core.projection-loop/apply-migration}
   :proj-intl-in-migrants      {:var #'witan.models.dem.ccm.mig.net-migration/project-international-in-migrants
                                :params {:start-yr-avg-inter-mig 2003
                                         :end-yr-avg-inter-mig 2014}}
   :proj-intl-out-migrants     {:var #'witan.models.dem.ccm.mig.net-migration/project-international-out-migrants
                                :params {:start-yr-avg-inter-mig 2003
                                         :end-yr-avg-inter-mig 2014}}
   :finish-looping?            {:var #'witan.models.dem.ccm.core.projection-loop/finish-looping?
                                :params {:last-proj-year 2015}}})

(def inputs
  {:in-historic-popn                  {:src "test_data/model_inputs/bristol_hist_popn_mye.csv"
                                       :key :historic-population}
   :in-historic-total-births          {:src "test_data/model_inputs/fert/bristol_hist_births_mye.csv"
                                       :key :historic-births}
   :in-projd-births-by-age-of-mother  {:src "test_data/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                                       :key :ons-proj-births-by-age-mother}
   :in-historic-deaths-by-age-and-sex {:src "test_data/model_inputs/mort/bristol_hist_deaths_mye.csv"
                                       :key :historic-deaths}
   :in-historic-dom-in-migrants       {:src "test_data/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                       :key :domestic-in-migrants}
   :in-historic-dom-out-migrants      {:src "test_data/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                       :key :domestic-out-migrants}
   :in-historic-intl-in-migrants      {:src "test_data/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                       :key :international-in-migrants}
   :in-historic-intl-out-migrants     {:src "test_data/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                                       :key :international-out-migrants}
   })

(def ccm-workflow
  [;; inputs for asfr
   [:in-historic-popn                 :calc-historic-asfr]
   [:in-historic-total-births         :calc-historic-asfr]
   [:in-projd-births-by-age-of-mother :calc-historic-asfr]

   ;; inputs for asmr
   [:in-historic-popn                  :calc-historic-asmr]
   [:in-historic-total-births          :calc-historic-asmr]
   [:in-historic-deaths-by-age-and-sex :calc-historic-asmr]

   ;; inputs for mig
   [:in-historic-dom-in-migrants   :proj-domestic-in-migrants]
   [:in-historic-dom-out-migrants  :proj-domestic-out-migrants]
   [:in-historic-intl-in-migrants  :proj-intl-in-migrants]
   [:in-historic-intl-out-migrants :proj-intl-out-migrants]

   ;; asfr/asmr projections
   [:calc-historic-asfr :proj-historic-asfr]
   [:calc-historic-asmr :proj-historic-asmr]

   ;; pre-loop merge
   [:proj-historic-asfr         :select-starting-popn]
   [:proj-historic-asmr         :select-starting-popn]
   [:proj-domestic-in-migrants  :select-starting-popn]
   [:proj-domestic-out-migrants :select-starting-popn]
   [:proj-intl-in-migrants      :select-starting-popn]
   [:proj-intl-out-migrants     :select-starting-popn]
   ;; - inputs for popn
   [:in-historic-popn           :select-starting-popn]

   ;; --- start popn loop
   [:select-starting-popn :project-births]
   [:select-starting-popn :project-deaths]
   [:project-births       :age-on]
   [:age-on               :add-births]
   [:add-births           :remove-deaths]
   [:project-deaths       :remove-deaths]
   [:remove-deaths        :apply-migration]
   [:apply-migration      :join-popn-latest-yr]
   [:join-popn-latest-yr  [:finish-looping? :out :select-starting-popn]]
   ;; --- end loop
   ])

(defn run-job*
  ([job inputs n]
   (let [{:keys [env-config
                 peer-config]} config
         {:keys [out]} (get-core-async-channels job)]
     ;;
     (doseq [[k {:keys [src key]}] inputs]
       (let [ch (get (get-core-async-channels job) k)
             data (load-dataset key src)]
         (if data
           (spool [data :done] ch)
           (throw (Exception. (str "Failed to load data: " src))))))

     (with-test-env [test-env [n env-config peer-config]]
       (onyx.test-helper/validate-enough-peers! test-env job)
       (let [job-id (:job-id (onyx.api/submit-job peer-config job))
             result (<!! out)]
         (onyx.api/await-job-completion peer-config job-id)
         result)))))

(defn add-inputs-and-outputs
  [input-keys job]
  (-> (reduce (fn [job' next-input] (add-task job' (core-async/input next-input (:batch-settings config)))) job input-keys)
      (add-task (core-async/output :out (:batch-settings config)))))

(defn workflow-fn-to-catalog
  "Take a defworkflowfn var and convert to a witan workspace catalog entry"
  ([task-name var params]
   (let [{:keys [witan/doc witan/param-schema]} (or (get (meta var) :witan/workflowfn)
                                                    (get (meta var) :witan/workflowpred))]
     (merge
      (when params
        (if param-schema
          {:witan/params (s/validate param-schema params)}
          (throw (Exception. (str task-name " has params but no param-schema")))))
      {:witan/name task-name
       :witan/fn (-> var (str) (subs 2) (keyword))})))
  ([task-name var]
   (workflow-fn-to-catalog task-name var nil)))

(def ccm-catalog
  (mapv (fn [[k {:keys [var params]}]] (workflow-fn-to-catalog k var params)) tasks))

(deftest workspace-test
  (let [onyx-job (add-inputs-and-outputs
                  (keys inputs)
                  (o/workspace->onyx-job
                   {:workflow ccm-workflow
                    :catalog ccm-catalog}
                   config))]
    (println (keys (run-job* onyx-job inputs (->> onyx-job
                                                  :workflow
                                                  (mapcat identity)
                                                  (set)
                                                  (count)))))))
