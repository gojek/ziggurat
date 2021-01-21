#!/usr/bin/env bash

set -ex

lein clean
mv -fv resources/config.test.{ci.edn,edn}
make test