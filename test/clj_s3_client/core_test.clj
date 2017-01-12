(ns clj-s3-client.core-test
  (:require [clojure.test :refer :all]
            [clj-containment-matchers.clojure-test :refer :all]
            [clj-http.client :as client]
            [clj-s3-client.core :refer :all]))

(defonce client (create-client))
(def bucket-name "adtile-ci-aws-sandbox-development")
(def object-key "clj-s3-client/store-file-to-s3.txt")
(defn sample-txt [] (clojure.java.io/input-stream (clojure.java.io/resource "sample.txt")))

(defn clean-after-test [suite]
  (suite)
  (delete-object client bucket-name object-key))

(use-fixtures :each clean-after-test)

(defn- ->metadata [& {:keys [content-type content-length custom-header content-encoding acl]
                    :or {content-type "text/plain"
                         content-length 7
                         custom-header "cats"
                         content-encoding "text"
                         acl :public-read}}]
  {:content-type content-type
   :content-length content-length
   :custom-header custom-header
   :content-encoding content-encoding
   :acl acl})

(defn- file->status [client bucket-name object-key]
  (:status (client/get (.getResourceUrl client bucket-name object-key) {:throw-exceptions false})))

(deftest move-object-around
  (testing "store file to s3 with public permission"
    (let [input-stream (sample-txt)]
      (put-object client bucket-name object-key input-stream (->metadata))
      (is (equal? (get-object client bucket-name object-key)
                  {:accept-ranges "bytes"
                   :bucket-name bucket-name
                   :content #(= "lolbal\n" (slurp %1))
                   :content-length 7
                   :content-type "text/plain"
                   :content-encoding "text"
                   :e-tag string?
                   :custom-header "cats"
                   :last-modified #(instance? java.util.Date %1)
                   :object-key object-key}))
      (is (= (file->status client bucket-name object-key) 200))))
  (testing "store file to s3 with private permission"
    (let [input-stream (sample-txt)]
      (put-object client bucket-name object-key input-stream (->metadata :custom-header "dogs" :acl :private))
      (is (equal? (get-object client bucket-name object-key)
                  {:accept-ranges "bytes"
                   :bucket-name bucket-name
                   :content #(= "lolbal\n" (slurp %1))
                   :content-length 7
                   :content-type "text/plain"
                   :content-encoding "text"
                   :e-tag string?
                   :custom-header "dogs"
                   :last-modified #(instance? java.util.Date %1)
                   :object-key object-key}))
      (is (= (file->status client bucket-name object-key) 403)))))

