#!/usr/bin/env bash

set -ex

lein clean
mv -fv resources/config.test.{cluster.ci.edn,cluster.edn}
make test-cluster
