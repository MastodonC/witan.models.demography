(ns witan.models.workspace-test
  (:require  [clojure.test :refer :all]
             [schema.core :as s]
             [clojure.core.async :refer [chan >!! <!! close!]]
             [onyx.plugin.core-async :refer [take-segments!]]
             [onyx.api]
             [witan.workspace.onyx :as o]
             [witan.workspace-api :refer [defworkflowfn]]
             [witan.workspace-api.onyx :refer [default-fn-wrapper
                                               default-pred-wrapper]]))

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

   ;; migration projections
   [:proj-domestic-in-migrants  :mig-net-flows]
   [:proj-domestic-out-migrants :mig-net-flows]
   [:proj-intl-in-migrants      :mig-net-flows]
   [:proj-intl-out-migrants     :mig-net-flows]

   ;; pre-loop merge
   [:proj-historic-asfr :select-starting-popn]
   [:proj-historic-asmr :select-starting-popn]
   [:mig-net-flows      :select-starting-popn]
   ;; - inputs for popn
   [:in-historic-popn   :select-starting-popn]

   ;; --- start popn loop
   [:select-starting-popn :project-births]
   [:select-starting-popn :project-deaths]
   [:project-births       :age-on]
   [:age-on               :add-births]
   [:add-births           :remove-deaths]
   [:project-deaths       :remove-deaths]
   [:remove-deaths        :apply-migration]
   [:apply-migration      :join-popn-latest-yr]
   [:join-popn-latest-yr  [:not-last-year? :incr-year :out]]
   [:incr-year            :select-starting-popn]
   ;; --- end loop
   ])
