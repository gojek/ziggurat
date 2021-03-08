#!/usr/bin/env bash

set -ex

lein clean
mv -fv resources/config.test.{cluster.ci.edn,cluster.edn}
sudo make test-cluster
