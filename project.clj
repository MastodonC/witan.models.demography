(defproject witan.models.demography "0.1.1-SNAPSHOT"
  :description "witan.models.demography is a Clojure library to run demographic models."
  :url "https://github.com/MastodonC/witan.models.demography"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [prismatic/schema "1.1.3"]
                 [net.mikera/core.matrix "0.52.2"]
                 [org.clojure/data.csv "0.1.3"]
                 [witan.workspace-api "0.1.20-SNAPSHOT"]
                 [org.clojure/tools.cli "0.3.5"]
                 [com.taoensso/timbre "4.7.3"]]
  :target-path "target/%s"
  :profiles {:dev {:source-paths ["src" "src-cli"]
                   :dependencies [[witan.phyrexian "0.1.0-SNAPSHOT"]
                                  [witan.workspace-executor "0.2.6"
                                   :exclusions [witan.workspace-api]]]}
             :uberjar {:aot :all}
             :cli {:main witan.models.run-models
                   :source-paths ["src" "src-cli"]
                   :dependencies [[witan.workspace-executor "0.2.6"
                                   :exclusions [witan.workspace-api]]]}
             :data {:source-paths ["src-data"]
                    :dependencies [[amazonica "0.3.73"]
                                   [me.raynes/fs "1.4.6"]]}}
  :exclusions [prismatic/schema org.clojure/clojure]
  :aliases {"split-data"  ["with-profile" "data" "run" "-m" "witan.models.split-data"]
            "upload-data" ["with-profile" "data" "run" "-m" "witan.models.upload-data"]}
  :repositories [["releases" {:url "https://clojars.org/repo"
                              :creds :gpg}]
                 ["snapshots" {:url "https://clojars.org/repo"
                               :creds :gpg}]])
