#!/usr/bin/env bash

docker run -dit -p 15672:15672 -p 5672:5672 rabbitmq:3.8.2-management-alpine
sleep 10
lein code-coverage