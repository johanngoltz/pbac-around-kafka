#!/bin/sh
nc --listen 8080 > payload
/usr/bin/kafka-console-producer \
  --topic 'bench' \
  --request-required-acks 1 \
  --producer-props bootstrap.servers=kafka-0:9092