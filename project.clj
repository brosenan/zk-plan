(defproject zk-plan "0.0.1-SNAPSHOT"
  :description "Cool new project to do things and stuff"
  :dependencies [[org.clojure/clojure "1.8.0"] [zookeeper-clj "0.9.4"]]
  :profiles {:dev {:dependencies [[midje "1.8.3"] [helpshift/hydrox "0.1.15"]]
                   :plugins [[lein-midje "3.2.1"] [lein-hydrox "0.1.17"]]}} 
  :documentation {:site   "sample"
                  :output "docs"
                  :template {:path "template"
                             :copy ["assets"]
                             :defaults {:template     "article.html"
                                        :navbar       [:file "partials/navbar.html"]
                                        :dependencies [:file "partials/deps-web.html"]
                                        :navigation   :navigation
                                        :article      :article}}
                  :paths ["test/zk-plan"]
                  :files {"index"
                          {:input "test/zk_plan/core_test.clj"
                           :title "API Documentation"
                           :subtitle "generating a document from code"}}
                  :link {:auto-tag    true
                         :auto-number  true}})

