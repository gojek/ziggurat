#!/usr/bin/env bash

set -ex

lein clean
make test-cluster
