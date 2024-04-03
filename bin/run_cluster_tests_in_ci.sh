#!/usr/bin/env bash

set -ex

lein clean
sudo make test-cluster
