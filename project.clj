(defproject clj-s3-client "1.0.0"
  :description "Clojure AWS s3 client"
  :url "https://github.com/adtile/clj-s3-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [camel-snake-kebab "0.4.0"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.75"]]
  :profiles {:dev {:dependencies [[clj-containment-matchers "1.0.1"]
                                  [clj-http "3.4.1"]]
                   :resource-paths ["dev-resources"]}}
  :signing {:gpg-key "webmaster@adtile.me"})
