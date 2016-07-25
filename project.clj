(defproject witan.models.demography "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [prismatic/schema "1.1.2"]
                 [net.mikera/core.matrix "0.51.0"]
                 [kixi/incanter-core "1.9.1-p0-3bf644a"]
                 [org.clojure/data.csv "0.1.3"]
                 [witan.workspace-api "0.1.12"]
                 [org.clojure/tools.cli "0.3.5"]
                 [com.taoensso/timbre "4.7.0"]]
  :target-path "target/%s"
  :profiles {:dev {:source-paths ["src" "src-cli"]
                   :dependencies [[witan.workspace-executor "0.2.2"
                                   :exclusions [witan.workspace-api]]]}
             :uberjar {:aot :all}
             :cli {:main witan.models.run-models
                   :source-paths ["src" "src-cli"]
                   :dependencies [[witan.workspace-executor "0.2.1"
                                   :exclusions [witan.workspace-api]]]}}
  :exclusions [prismatic/schema org.clojure/clojure])
