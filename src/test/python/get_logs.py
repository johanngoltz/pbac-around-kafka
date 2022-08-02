import os
from collections import namedtuple
from google.cloud import logging
import re
import csv
from itertools import groupby
import dateutil.parser

BenchmarkEntry = namedtuple("BenchmarkEntry", ("container_id", "timestamp", "message"))
BenchmarkResult = namedtuple("BenchmarkResult", ("container_id", "timestamp", "records_per_second", "avg_latency"))

resource_benchmark_results = "projects/pbac-in-pubsub/locations/us-central1/buckets/benchmark-results/views/_AllLogs"


def _get_benchmark_entries(logging_client, produce_or_consume, benchmark_start_timestamp, benchmark_end_timestamp):
    return (
        BenchmarkEntry(container_id=entry.payload.get("cos.googleapis.com/container_id"),
                       timestamp=entry.timestamp,
                       message=entry.payload.get("message"))
        for entry in
        logging_client.list_entries(
            resource_names=[resource_benchmark_results],
            filter_=_get_filter_string(produce_or_consume, benchmark_start_timestamp, benchmark_end_timestamp))
    )


def _get_filter_string(produce_or_consume, benchmark_start_timestamp, benchmark_end_timestamp):
    if produce_or_consume == "Produce":
        is_progress_message = 'jsonPayload.message =~ "^\\d+ records"'
    else:
        is_progress_message = 'jsonPayload.message =~ "^\\d{4}-\\d\\d-\\d\\d [\\d:]{12},"'
    is_after_begin = f'timestamp >= "{benchmark_start_timestamp}"'
    is_before_end = f'timestamp <= "{benchmark_end_timestamp}"'
    return is_progress_message + " " + is_after_begin + " " + is_before_end


def get_timeseries_from_logs(produce_or_consume: str, benchmark_start_timestamp: str, benchmark_end_timestamp: str, file_name: str):
    logging_client = logging.Client(project="pbac-in-pubsub")

    benchmark_entries = _get_benchmark_entries(logging_client, produce_or_consume, benchmark_start_timestamp, benchmark_end_timestamp)

    regex = re.compile(".*?(?P<RecordsPerSecond>[\\d,.]+) records/sec.*?(?P<AvgLatency>[\\d,.]+) ms avg latency")

    def by_container_id(t: BenchmarkEntry):
        return t.container_id

    result = []
    for container_id, logs in groupby(sorted(benchmark_entries, key=by_container_id), key=by_container_id):
        if produce_or_consume == "Produce":
            for entry in logs:
                entry: BenchmarkEntry = entry
                match = regex.match(entry.message)
                # todo overall records
                records_per_second = float(match.group("RecordsPerSecond"))
                avg_latency = float(match.group("AvgLatency"))
                result.append({
                    "Producer": container_id,
                    "Timestamp": entry.timestamp,
                    "RecordsPerSecond": records_per_second,
                    "AvgLatency": avg_latency
                })
        else:
            pbac_reader = csv.DictReader((l.message for l in logs), fieldnames=["time", "threadId", "data.consumed.in.MB", "MB.sec", "data.consumed.in.nMsg", "nMsg.sec", "rebalance.time.ms", "fetch.time.ms", "fetch.MB.sec", "fetch.nMsg.sec"])
            for row in pbac_reader:
                result.append({
                    "Producer": container_id,
                    "Timestamp": dateutil.parser.parse(row["time"][:-4] + "Z"),
                    "RecordsPerSecond": float(row["nMsg.sec"])
                })

    os.makedirs("results", exist_ok=True)

    with open(file_name, "x", newline="") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=("Producer", "Timestamp", "RecordsPerSecond", "AvgLatency"))

        writer.writeheader()
        writer.writerows(result)

    return result
