(ns witan.models.workspace-test
  (:require  [clojure.test :refer :all]
             [schema.core :as s]
             [clojure.core.async :refer [chan >!! <!! close!]]
             [onyx.plugin.core-async :refer [take-segments!]]
             [onyx.api]
             [witan.workspace.onyx :as o]
             [witan.workspace-api :refer [defworkflowfn]]
             [witan.workspace-api.onyx :refer [default-fn-wrapper
                                               default-pred-wrapper]]
             ;;
             [witan.models.dem.ccm.fert.fertility-mvp]
             [witan.models.dem.ccm.mort.mortality-mvp]
             [witan.models.dem.ccm.mig.net-migration]
             [witan.models.dem.ccm.core.projection-loop]))

(defn workspace
  [{:keys [workflow contracts catalog] :as raw}]
  (->
   raw
   (assoc :workflow (or workflow []))
   (assoc :contracts (or contracts []))
   (assoc :catalog (or catalog []))))

(def config
  {:redis-config {:redis/uri "redis"}
   :batch-settings {:onyx/batch-size 1
                    :onyx/batch-timeout 1000}
   :fn-wrapper :witan.workspace-api.onyx/default-fn-wrapper
   :pred-wrapper :witan.workspace-api.onyx/default-pred-wrapper})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/tenancy-id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :witan.models.workspace-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :witan.models.workspace-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def batch-size 10)

(def catalog-entry-in
  {:onyx/name :in
   :onyx/plugin :onyx.plugin.core-async/input
   :onyx/type :input
   :onyx/medium :core.async
   :onyx/batch-size batch-size
   :onyx/max-peers 1
   :in/chan "foo"
   :onyx/doc "Reads segments from a core.async channel"})

(def catalog-entry-out
  {:onyx/name :out
   :onyx/plugin :onyx.plugin.core-async/output
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/batch-size batch-size
   :onyx/max-peers 1
   :onyx/doc "Writes segments to a core.async channel"})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defworkflowfn inc-test
  "This is a test function which will increment a number
  passed into it."
  {:witan/name :_
   :witan/version "1.0"
   :witan/input-schema {:number s/Int}
   :witan/output-schema {:number s/Int}}
  [{:keys [number]} _]
  {:number (inc number)})

(defworkflowfn duplicate-test
  "This is a test function which will duplicate a key
  into another key"
  {:witan/name :_
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/param-schema {:from s/Keyword
                        :to s/Keyword}
   :witan/output-schema {:* s/Any}}
  [m {:keys [from to]}]
  (hash-map to (get m from)))

(deftest testing-a-model
  (let [input-segments  [{:number 0}
                         {:number 1}
                         {:number 2}
                         {:number 3}
                         {:number 4}
                         {:number 5}
                         :done]
        workflow [[:in :inc]
                  [:inc :dupe]
                  [:dupe :out]]
        onyx-job
        (o/workspace->onyx-job
         (workspace
          {:workflow workflow
           :catalog [{:witan/name :inc
                      :witan/fn :witan.models.workspace-test/inc-test}
                     {:witan/name :dupe
                      :witan/fn :witan.models.workspace-test/duplicate-test
                      :witan/params {:from :number :to :foo}}]}) config)
        onyx-job' (-> onyx-job
                      (assoc :lifecycles lifecycles
                             :flow-conditions [])
                      (update :catalog conj catalog-entry-in)
                      (update :catalog conj catalog-entry-out))]

    (testing "Run local onyx job"
      (doseq [segment input-segments]
        (>!! input-chan segment))
      (close! input-chan)
      (let [env        (onyx.api/start-env env-config)
            peer-group (onyx.api/start-peer-group peer-config)
            n-peers    (count (set (mapcat identity workflow)))
            v-peers    (onyx.api/start-peers n-peers peer-group)
            _          (onyx.api/submit-job
                        peer-config
                        onyx-job')
            results (take-segments! output-chan)]

        (doseq [v-peer v-peers]
          (onyx.api/shutdown-peer v-peer))
        (onyx.api/shutdown-peer-group peer-group)
        (onyx.api/shutdown-env env)

        (is (= results [{:number 1 :foo 1}
                        {:number 2 :foo 2}
                        {:number 3 :foo 3}
                        {:number 4 :foo 4}
                        {:number 5 :foo 5}
                        {:number 6 :foo 6}
                        :done]))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Real Model Test

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
   ;;:incr-year                  {:var nil}
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
   :finish-looping?              {:var #'witan.models.dem.ccm.core.projection-loop/finish-looping?
                                  :params {:last-proj-year 2015}}})

(def inputs
  {:in-historic-popn                  {:chan (chan)
                                       :src "test_data/model_inputs/bristol_hist_popn_mye.csv"}
   :in-projd-births-by-age-of-mother  {:chan (chan)
                                       :src "test_data/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"}
   :in-historic-total-births          {:chan (chan)
                                       :src "test_data/model_inputs/fert/bristol_hist_births_mye.csv"}
   :in-historic-deaths-by-age-and-sex {:chan (chan)
                                       :src "test_data/model_inputs/mort/bristol_hist_deaths_mye.csv"}
   :in-historic-dom-in-migrants       {:chan (chan)
                                       :src "test_data/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"}
   :in-historic-dom-out-migrants      {:chan (chan)
                                       :src "test_data/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"}
   :in-historic-intl-in-migrants      {:chan (chan)
                                       :src "test_data/model_inputs/mig/bristol_hist_international_inmigrants.csv"}
   :in-historic-intl-out-migrants     {:chan (chan)
                                       :src "test_data/model_inputs/mig/bristol_hist_international_outmigrants.csv"}})

(defn inject-channel [event lifecycle]
  {:core.async/chan (get-in event [:onyx.core/task-map :in/chan])})

(def input-calls
  {:lifecycle/before-task-start inject-channel})

(defn input-to-lifecycle
  [input]
  [{:lifecycle/task input
    :lifecycle/calls :witan.models.workspace-test/input-calls}
   {:lifecycle/task input
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}])

(defn input-to-catalog
  [ch batch-size input]
  {:onyx/name input
   :onyx/plugin :onyx.plugin.core-async/input
   :onyx/type :input
   :onyx/medium :core.async
   :onyx/batch-size batch-size
   :onyx/max-peers 1
   :in/chan ch
   :onyx/doc "Reads segments from a core.async channel"})

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

(def ccm-batch-size 1)

(def ccm-catalog
  (mapv (fn [[k {:keys [var params]}]] (workflow-fn-to-catalog k var params)) tasks))

;; (mapv (fn [[k v]] (input-to-catalog (:chan v) ccm-batch-size k)) inputs)

(def ccm-workflow
  [;; inputs for asfr
   [:in-historic-popn                 :calc-historic-asfr]
   [:in-projd-births-by-age-of-mother :calc-historic-asfr]
   [:in-historic-total-births         :calc-historic-asfr]

   ;; inputs for asmr
   [:in-historic-popn                  :calc-historic-asmr]
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
   [:join-popn-latest-yr  :out]
   ;; --- end loop
   ])

(deftest testing-a-model
  (let [state {}
        onyx-job (o/workspace->onyx-job
                  (workspace
                   {:workflow ccm-workflow
                    :catalog ccm-catalog}) config)
        onyx-job' (-> onyx-job
                      (assoc :lifecycles lifecycles)
                      (update :catalog conj catalog-entry-in)
                      (update :catalog conj catalog-entry-out))]
    (clojure.pprint/pprint onyx-job')

    #_(testing "Run local onyx job"
        (doseq [segment input-segments]
          (>!! input-chan segment))
        (close! input-chan)
        (let [env        (onyx.api/start-env env-config)
              peer-group (onyx.api/start-peer-group peer-config)
              n-peers    (count (set (mapcat identity workflow)))
              v-peers    (onyx.api/start-peers n-peers peer-group)
              _          (onyx.api/submit-job
                          peer-config
                          onyx-job')
              results (take-segments! output-chan)]

          (doseq [v-peer v-peers]
            (onyx.api/shutdown-peer v-peer))
          (onyx.api/shutdown-peer-group peer-group)
          (onyx.api/shutdown-env env)

          (is (= results [{:number 1 :foo 1}
                          {:number 2 :foo 2}
                          {:number 3 :foo 3}
                          {:number 4 :foo 4}
                          {:number 5 :foo 5}
                          {:number 6 :foo 6}
                          :done]))))))
