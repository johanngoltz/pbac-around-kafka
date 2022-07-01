nc --listen 8080 > payload
./kafka-producer-perf-test --num-records $BENCHMARK_NUM_RECORDS \
  --topic 'producer-bench' \
  --producer-props 'bootstrap.servers=kafka-0:9092' \
  --payload-file payload \
  --throughput -1