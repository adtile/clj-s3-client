(ns clj-s3-client.core-test
  (:require [clojure.test :refer :all]
            [clj-containment-matchers.clojure-test :refer :all]
            [clj-s3-client.core :refer :all]))

(defonce client (create-client))
(def bucket-name "adtile-ci-aws-sandbox-development")
(def object-key "clj-s3-client/store-file-to-s3.txt")
(defn sample-txt [] (clojure.java.io/input-stream (clojure.java.io/resource "sample.txt")))

(defn clean-after-test [suite]
  (suite)
  (delete-object client bucket-name object-key))

(use-fixtures :each clean-after-test)

(deftest move-object-around
  (testing "store file to s3"
    (let [input-stream (sample-txt)
          content-length 7
          content-type "text/plain"
          content-encoding "gzip"]
      (put-object client bucket-name object-key input-stream {:content-type content-type :content-length content-length :custom-header "cats" :content-encoding content-encoding})
      (is (equal? (get-object client bucket-name object-key)
                  {:accept-ranges "bytes"
                   :bucket-name bucket-name
                   :content #(= "lolbal\n" (slurp %1))
                   :content-length 7
                   :content-type "text/plain"
                   :content-encoding content-encoding
                   :e-tag string?
                   :custom-header "cats"
                   :last-modified #(instance? java.util.Date %1)
                   :object-key object-key})))))

