FROM docker.io/confluentinc/cp-kafka:7.1.1

ENV BENCHMARK_NUM_RECORDS=50000000
ENV BENCH_KAFKA_HOST=kafka-0
ENV BENCH_KAFKA_PORT=9092
ENV BENCH_PURPOSES=billing,marketing
ENV BENCH_TOPIC=bench
ENV BENCH_NUM_DUMMY_RESERVATIONS=1000000
ENV BENCH_NEW_RESERVATION_FREQUENCY_SECONDS=300

USER root
RUN yum install -y zulu17-jre-headless

USER appuser
ADD run-producer-bench.sh /usr/bin/
ADD run-consumer-bench.sh /usr/bin/
ADD run-e2e-bench.sh /usr/bin/
ADD *-tests.jar /usr/bin/
ADD *-with-dependencies.jar /usr/bin/
