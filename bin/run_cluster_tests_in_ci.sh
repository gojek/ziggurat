#!/usr/bin/env bash

set -ex

lein clean


sudo PATH="$PATH" which lein
sudo PATH="$PATH" make test-cluster
