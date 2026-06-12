#!/usr/bin/env bash

set -ex

lein clean
which lein
lein --version
sudo which lein || true
sudo env | grep PATH
sudo make test-cluster
