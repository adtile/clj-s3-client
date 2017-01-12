(ns clj-s3-client.core
  "Functions to interact with Amazon S3 storage.

  All functions take AmazonS3Client instance as the first parameter.
  "
  (:require [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [camel-snake-kebab.extras :refer [transform-keys]]
            [clojure.set :refer [difference]])
  (:import [com.amazonaws.services.s3 AmazonS3Client AmazonS3URI]
           [com.amazonaws.services.s3.model CannedAccessControlList PutObjectRequest AmazonS3Exception ObjectMetadata]
           [com.amazonaws ClientConfiguration]
           [com.amazonaws.regions Regions]
           [java.io InputStream]
           [java.nio ByteBuffer]))

(defn- acl->access-control-list [acl]
  (get {:private CannedAccessControlList/Private
        :public-read CannedAccessControlList/PublicRead
        :public-read-write CannedAccessControlList/PublicReadWrite
        :aws-exec-read CannedAccessControlList/AwsExecRead
        :authenticated-read CannedAccessControlList/AuthenticatedRead
        :bucket-owner-read CannedAccessControlList/BucketOwnerRead
        :bucket-owner-full-control CannedAccessControlList/BucketOwnerFullControl
        :log-delivery-write CannedAccessControlList/LogDeliveryWrite} acl CannedAccessControlList/Private))

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

(defn- suppress-not-found-exception [exception]
  (when-not (= 404 (.getStatusCode exception))
    (throw exception)))

(defn create-client
  "## (create-client)

  Returns an instance of AmazonS3Client which can be used with all other functions to access S3 resources.

  An optional map of options can include any of the following keys:

    :max-connections    - the max number of active connections for client.
                          Defaults to 50.

    :max-error-retry    - the number of retries before failing.
                          Defaults to 1.

    :endpoint           - the endpoint to connect to.

    :region             - the region to be used.

    :tcp-keep-alive     - set the 'keep connection alive' boolean flag for http connections.
                          Defaults to false.

  "
  [& {:keys [max-connections max-error-retry endpoint region tcp-keep-alive]
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

(defn create-bucket
  "## (create-bucket client \"my-awesome-bucket\")

  Creates a bucket `bucket-name`.
  "
  [^AmazonS3Client client bucket-name]
  (.createBucket client bucket-name))

(defn delete-bucket
  "## (delete-bucket client \"my-not-so-awesome-bucket\")

  Deletes the bucket `bucket-name`.
  "
  [^AmazonS3Client client bucket-name]
  (.deleteBucket client bucket-name))

(defn exists?-bucket [^AmazonS3Client client bucket-name]
  "## (exists?-bucket client \"my-awesome-bucket\")

  Checks if bucket `bucket-name` exists.

  Returns `true` or `false`.
  "
  (.doesBucketExist client bucket-name))

(defn put-object
  "## (put-object client \"my-awesome-bucket\" \"my-awesome-me.txt\" file-input-stream {:acl :private :content-length 1234 :content-type \"text/html\"})

  Store the input stream to s3 with given options.

  An optional map of options can include any of the following keys:

    :acl                    - sets access rights of the file, supported values are
                                :private
                                :public-read
                                :public-read-write
                                :aws-exec-read
                                :authenticated-read
                                :bucket-owner-read
                                :bucket-owner-full-control
                                :log-delivery-write

    :content-length         - the length of the content in bytes.

    :content-type           - the mime type of the content (e.g \"image/png\").

    :content-language       - content language of the InputStream (e.g \"en\").

    :content-encoding       - the encoding of the content (e.g. gzip).

    :content-disposition    - how the content should be downloaded by browsers (e.g. attachment; filename=\"filename.jpg\").

    :http-expires-date      - when the content expires, used in cache headers (e.g. Sun, 7 May 1995 12:45:26 GMT).


  The map can also contain other keys which are added as custom headers for the file.

  "
  [^AmazonS3Client client bucket-name object-key ^InputStream is options]
  (let [metadata (dissoc options :acl)
        acl (:acl options)
        s3-object-metadata (map->object-metadata metadata)
        put-object-req (.withCannedAcl
                         (PutObjectRequest. bucket-name object-key is s3-object-metadata)
                         (acl->access-control-list acl))
        put-object-resp (.putObject client put-object-req)
        result-s3-object-metadata (object-metadata->map (.getMetadata put-object-resp))]
    (assoc result-s3-object-metadata :object-key object-key :bucket-name bucket-name :content is)))

(defn get-object
  "## (get-object client \"my-awesome-bucket\" \"my-awesome-me.txt\")

  Fetches the object from bucket `bucket-name` with the key `object-key`. Metadata and content (input stream) are included.

  Returns `nil` if the object is not found.

  "
  [^AmazonS3Client client bucket-name object-key]
  (try
    (let [s3-object (.getObject client bucket-name object-key)
          s3-object-metadata (object-metadata->map (.getObjectMetadata s3-object))]
      (assoc s3-object-metadata :object-key (.getKey s3-object) :bucket-name (.getBucketName s3-object) :content (.getObjectContent s3-object)))
    (catch AmazonS3Exception e (suppress-not-found-exception e))))

(defn delete-object
  "## (delete-object client \"my-awesome-bucket\" \"my-awesome-me.txt\")

  Destroys the object `object-key` from bucket `bucket-name`.

  "
  [^AmazonS3Client client bucket-name object-key]
  (.deleteObject client bucket-name object-key))

(defn bucket-name-and-object-key->url [^AmazonS3Client client bucket-name object-key]
  (str (.getUrl client bucket-name object-key)))

(defn url->bucket-name-and-object-key [url]
  (let [s3-uri (AmazonS3URI. url true)
        bucket-name (.getBucket s3-uri)
        object-key (.getKey s3-uri)]
    {:bucket-name bucket-name
     :object-key object-key}))

