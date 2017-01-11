(ns clj-s3-client.core
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [camel-snake-kebab.extras :refer [transform-keys]]
            [clojure.set :refer [difference]])
  (:import [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model AmazonS3Exception ObjectMetadata]
           [com.amazonaws ClientConfiguration]
           [com.amazonaws.regions Regions]
           [java.io InputStream]
           [java.nio ByteBuffer]))

(defn create-client [& {:keys [max-connections max-error-retry endpoint region tcp-keep-alive]
                        :or {max-connections 50 max-error-retry 1 tcp-keep-alive false}}]
  (let [configuration (-> (ClientConfiguration.)
                          (.withMaxErrorRetry max-error-retry)
                          (.withMaxConnections max-connections)
                          (.withTcpKeepAlive tcp-keep-alive))
        client (AmazonS3Client. configuration)]
    (when endpoint
      (.setEndpoint client endpoint))
    (if region
      (.withRegion client (Regions/fromName region))
      client)))

(defn create-bucket [^AmazonS3Client client bucket-name]
  (.createBucket client bucket-name))

(defn delete-bucket [^AmazonS3Client client bucket-name]
  (.deleteBucket client bucket-name))

(defn exists?-bucket [^AmazonS3Client client bucket-name]
  (.doesBucketExist client bucket-name))

(defn- object-metadata->map [^ObjectMetadata s3-object-metadata]
  (let [s3-object-raw-metadata (into {} (.getRawMetadata s3-object-metadata))
        s3-user-metadata (into {} (.getUserMetadata s3-object-metadata))]
    (transform-keys ->kebab-case-keyword (conj s3-object-raw-metadata s3-user-metadata))))

(defn- map->object-metadata [metadata]
  (let [object-metadata (ObjectMetadata.)
        supported {:content-length (fn [ob value] (doto ob (.setContentLength value)))
                   :content-type (fn [ob value] (doto ob (.setContentType value)))
                   :content-language (fn [ob value] (doto ob (.setContentLanguage value)))
                   :content-encoding (fn [ob value] (doto ob (.setContentEncoding value)))
                   :content-disposition (fn [ob value] (doto ob (.setContentDisposition value)))
                   :http-expires-date (fn [ob value] (doto ob (.setHttpExpiresDate value)))}
        specific-setters (select-keys supported (keys metadata))
        user-meta-fields (difference (set (keys metadata)) (set (keys specific-setters)))
        user-metadata-setter (reduce (fn [acc k] (assoc acc k (fn [ob value] (doto ob (.addUserMetadata (name k) value))))) {} user-meta-fields)]
    (reduce-kv
      (fn [ob k v] (v ob (get metadata k)))
      object-metadata
      (conj specific-setters user-metadata-setter))))

(defn put-object [^AmazonS3Client client bucket-name object-key ^InputStream is metadata]
  (let [s3-object-metadata (map->object-metadata metadata)
        put-object-response (.putObject client bucket-name object-key is s3-object-metadata)
        result-s3-object-metadata (object-metadata->map (.getMetadata put-object-response))]
    (assoc result-s3-object-metadata :object-key object-key :bucket-name bucket-name :content is)))

(defn- suppress-not-found-exception [exception]
  (when-not (= 404 (.getStatusCode exception))
    (throw exception)))

(defn get-object [^AmazonS3Client client bucket-name object-key]
  (try
    (let [s3-object (.getObject client bucket-name object-key)
          s3-object-metadata (object-metadata->map (.getObjectMetadata s3-object))]
      (assoc s3-object-metadata :object-key (.getKey s3-object) :bucket-name (.getBucketName s3-object) :content (.getObjectContent s3-object)))
    (catch AmazonS3Exception e (suppress-not-found-exception e))))

(defn delete-object [^AmazonS3Client client bucket-name object-key]
  (.deleteObject client bucket-name object-key))


