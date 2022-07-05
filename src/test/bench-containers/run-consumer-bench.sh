#!/bin/sh
nc --listen 8080
/usr/bin/kafka-consumer-perf-test --messages $BENCHMARK_NUM_RECORDS \
  --topic 'bench'\
  --bootstrap-server 'kafka-0:9092' \
  --consumer.config 'NoCheckCrcs.config' \
  --timeout 60000 \
  --show-detailed-stats