FROM docker.io/confluentinc/cp-kafka:7.1.1

ENV BENCHMARK_NUM_RECORDS=50000000

ADD run-producer-bench.sh /usr/bin/
ADD run-consumer-bench.sh /usr/bin/
ADD run-latency-bench-producer.sh /usr/bin/
ADD run-latency-bench-consumer.sh /usr/bin/
