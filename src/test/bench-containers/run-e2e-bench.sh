#!/bin/sh

CLASSPATH=/usr/bin/* /usr/bin/kafka-run-class purposeawarekafka.benchmark.e2e.E2eBenchmark
# cat *.csv |ncat controller $((8080 + $(echo $HOSTNAME | cut -c 5)))
