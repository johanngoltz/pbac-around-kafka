import plotly.express as px
import pandas as pd
import os
import plotly.io as pio
from get_logs import get_timeseries_from_logs

from pandas import DataFrame

pio.kaleido.scope.default_scale = 4


def make_scatterplot(i, df, file_name):
    fig = px.scatter(df, x="seconds_since_start", y="RecordsPerSecond", color="plain_or_pbac", trendline="lowess",
                     title=f"Throughput over time, {i} producers",
                     labels={"RecordsPerSecond": "Throughput [Records/s]"})
    fig.show()

    fig.write_image(f"images/{file_name}.png")


def make_barchart(df: pd.DataFrame):
    fig = px.bar(df,
                 x=df.index.get_level_values("client_count"),
                 y="RecordsPerSecond",
                 color=df.index.get_level_values("plain_or_pbac"),
                 title="Average Throughput",
                 barmode="group",
                 labels=dict(x="#Clients", color="Mode"))
    fig.update_xaxes(type='category')

    fig.show()


if __name__ == "__main__":
    if not os.path.exists("images"):
        os.mkdir("images")

    runs: DataFrame = pd.read_csv("BenchmarkRuns.csv", sep=";", parse_dates=["begin_ts", "end_ts"],
                                  names=("produce_or_consume", "plain_or_pbac", "client_count", "begin_ts", "end_ts"))

    plain_or_pbac = "Plain"
    produce_or_consume = "Produce"

    candidate_indices = runs[runs["plain_or_pbac"] == plain_or_pbac]["produce_or_consume"] == produce_or_consume

    maxes = runs[candidate_indices].groupby("client_count").max()

    all_data = None

    for client_count in maxes.T:
        run = dict(maxes.loc[client_count])
        file_format_begin_ts = run.get('begin_ts').strftime("%Y%m%dT%H%M%S%z")

        run["begin_ts"] = run.get("begin_ts").isoformat()
        run["end_ts"] = run.get("end_ts").isoformat()

        file_name = f"{run.get('produce_or_consume')}.{run.get('plain_or_pbac')}.{client_count}.{file_format_begin_ts}"
        csv_file_name = f"results/{file_name}.csv"

        if not os.path.exists(csv_file_name):
            get_timeseries_from_logs(run.get('begin_ts'), run.get('end_ts'), csv_file_name)

        df = pd.read_csv(csv_file_name, parse_dates=["Timestamp"])
        df["plain_or_pbac"] = plain_or_pbac
        df["produce_or_consume"] = produce_or_consume
        df["seconds_since_start"] = (df["Timestamp"] - maxes.loc[client_count]["begin_ts"]).map(
            lambda x: x.total_seconds())
        df["client_count"] = client_count

        make_scatterplot(client_count, df, file_name)

        all_data = pd.concat((all_data, df))

    avg_sum_throughput = all_data \
        .query("seconds_since_start > 400") \
        .groupby(["plain_or_pbac", "produce_or_consume", "client_count"]) \
        .mean() \
        ["RecordsPerSecond"] \
        .multiply(list(maxes.T)) \
        .to_frame("RecordsPerSecond")

    make_barchart(avg_sum_throughput)
