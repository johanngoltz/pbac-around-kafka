#!/bin/sh
nc --listen 8080 > payload
/usr/bin/kafka-producer-perf-test --num-records $BENCHMARK_NUM_RECORDS \
  --topic 'bench' \
  --producer-props batch.size=65356 bootstrap.servers=kafka-0:9092 linger.ms=1000 \
  --payload-file payload \
  --throughput -1