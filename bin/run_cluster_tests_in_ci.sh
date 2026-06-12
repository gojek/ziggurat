#!/usr/bin/env bash

set -ex

lein clean
echo "PATH before sudo:"
echo $PATH

sudo PATH="$PATH" which lein
sudo PATH="$PATH" make test-cluster
