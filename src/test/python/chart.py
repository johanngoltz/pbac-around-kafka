import plotly.express as px
import pandas as pd
import os
import plotly.io as pio
from get_logs import get_timeseries_from_logs

from pandas import DataFrame

pio.kaleido.scope.default_scale = 4


def make_scatterplot(i, df):
    return px.scatter(df, x="seconds_since_start", y="RecordsPerSecond", color="plain_or_pbac", trendline="lowess",
                      title=f"Throughput over time, {i} producers",
                      labels={"RecordsPerSecond": "Throughput [Records/s]"})


def make_barchart(df: pd.DataFrame):
    fig = px.bar(df,
                 x=df.index.get_level_values("client_count"),
                 y="RecordsPerSecond",
                 color=df.index.get_level_values("plain_or_pbac"),
                 title="Average Throughput",
                 barmode="group",
                 labels=dict(x="#Clients", color="Mode"))
    fig.update_xaxes(type='category')
    return fig


if __name__ == "__main__":
    if not os.path.exists("images"):
        os.mkdir("images")

    runs: DataFrame = pd.read_csv("BenchmarkRuns.csv", sep=";", parse_dates=["begin_ts", "end_ts"],
                                  names=("produce_or_consume", "plain_or_pbac", "client_count", "begin_ts", "end_ts"))

    plain_or_pbac = "Plain"
    produce_or_consume = "Produce"

    candidate_indices = runs[runs["plain_or_pbac"] == plain_or_pbac]["produce_or_consume"] == produce_or_consume

    group_by = ["plain_or_pbac", "produce_or_consume", "client_count"]
    latest_runs = runs[candidate_indices].groupby(group_by).max()

    all_data = None

    for runtuple in latest_runs.itertuples():
        begin_ts = runtuple.begin_ts
        end_ts = runtuple.end_ts
        plain_or_pbac, produce_or_consume, client_count = runtuple.Index

        file_format_begin_ts = begin_ts.strftime("%Y%m%dT%H%M%S%z")

        begin_ts = begin_ts.isoformat()
        end_ts = end_ts.isoformat()

        file_name = f"{produce_or_consume}.{plain_or_pbac}.{client_count}.{file_format_begin_ts}"
        csv_file_name = f"results/{file_name}.csv"

        if not os.path.exists(csv_file_name):
            get_timeseries_from_logs(begin_ts, end_ts, csv_file_name)

        df = pd.read_csv(csv_file_name, parse_dates=["Timestamp"])
        df["plain_or_pbac"] = plain_or_pbac
        df["produce_or_consume"] = produce_or_consume
        df["seconds_since_start"] = (df["Timestamp"] - runtuple.begin_ts).map(lambda x: x.total_seconds())
        df["client_count"] = client_count

        fig = make_scatterplot(client_count, df)
        fig.write_image(f"images/{file_name}.png")

        all_data = pd.concat((all_data, df))

    avg_throughput = all_data \
        .query("seconds_since_start > 400") \
        .groupby(group_by) \
        .mean() \
        ["RecordsPerSecond"]
    avg_sum_throughput = avg_throughput \
        .multiply(avg_throughput.index.get_level_values("client_count")) \
        .to_frame("RecordsPerSecond")

    fig = make_barchart(avg_sum_throughput)
    fig.write_image(f"images/SumThroughput.png")
