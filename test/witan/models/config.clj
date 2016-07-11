(ns witan.models.config
  (:require [witan.workspace-api.onyx :refer [default-fn-wrapper default-pred-wrapper]]))

(def config
  {:env-config {:onyx/tenancy-id "testcluster"
                :onyx.bookkeeper/server? false
                :zookeeper/address "127.0.0.1:2188"
                :zookeeper/server? true
                :zookeeper.server/port 2188}
   :peer-config {:onyx/tenancy-id                       "testcluster"
                 :zookeeper/address                     "127.0.0.1:2188"
                 :onyx.peer/job-scheduler               :onyx.job-scheduler/greedy
                 :onyx.peer/zookeeper-timeout           60000
                 :onyx.messaging/allow-short-circuit?   true ;; TODO should be false on multinode
                 :onyx.messaging/impl                   :aeron
                 :onyx.messaging/bind-addr              "localhost"
                 :onyx.messaging/peer-port              40200
                 :onyx.messaging.aeron/embedded-driver?  true ;; TODO false on multinode
                 :onyx.messaging.aeron/publication-creation-timeout 10000
                 :onyx.messaging/backpressure-strategy :high-restart-latency
                 :onyx.log/config {:level :debug}}
   :redis-config {:redis/uri "redis://localhost:6379"}
   :batch-settings {:onyx/batch-size 1
                    :onyx/batch-timeout 1000}
   :task-settings {:onyx/n-peers 1}
   :fn-wrapper :witan.workspace-api.onyx/default-fn-wrapper
   :pred-wrapper :witan.workspace-api.onyx/default-pred-wrapper})
