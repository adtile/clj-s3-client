# clj-s3-client

[![Build Status](https://travis-ci.org/adtile/clj-s3-client.svg?branch=master)](https://travis-ci.org/adtile/clj-s3-client)
[![Clojars Project](https://img.shields.io/clojars/v/clj-s3-client.svg)](https://clojars.org/clj-s3-client)

A minimalistic Clojure wrapper for AWS s3 client.

[Documentation](https://rawgit.com/adtile/clj-s3-client/master/docs/uberdoc.html "Documentation")

## Usage

```clojure

(require [clj-s3-client.core :refer :all])

(let [client (create-client)]
  (create-bucket client "bucket-name")

(let [client (create-client)]
  (delete-bucket client "bucket-name")

(let [client (create-client)]
  (put-object client "bucket-name" "object-key" is {:meta {}}))

(let [client (create-client)]
  (get-object client "bucket-name" "object-key"))

(let [client (create-client)]
  (delete-object client "bucket-name" "object-key"))

```

## Release

```bash

# Release to clojars
lein deploy clojars
# Generate docs
lein marg
# Push to github.


```

## License

Copyright © 2017 Adtile Technologies Inc.

Distributed under the Eclipse Public License version 1.0.
