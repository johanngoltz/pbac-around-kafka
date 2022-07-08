import itertools

import plotly.express as px
import plotly.graph_objects as go
import statsmodels
import pandas as pd
import os
import argparse
from get_logs import get_timeseries_from_logs

from pandas import DataFrame


def read_df(i: int, start_ts: str, produce_or_consume: str):
    df_pbac = pd.read_csv(f"results/{produce_or_consume}.PBAC.{i}.{start_ts}.csv")
    df_pbac["System Under Test"] = 'PBAC / 5M records'
    df_plain = pd.read_csv(f"results/{produce_or_consume}.Plain.{i}.{start_ts}.csv")
    df_plain["System Under Test"] = 'Plain Kafka / 10M records'
    return {"pbac": df_pbac, "plain": df_plain}


def make_scatterplot(i, df):
    fig = px.scatter(df, x="seconds_since_start", y="RecordsPerSecond", color="plain_or_pbac", trendline="lowess",
                     title=f"Throughput over time, {i} producers",
                     labels={"RecordsPerSecond": "Throughput [Records/s]"})
    fig.show()

    if not os.path.exists("images"):
        os.mkdir("images")

    # fig.write_image(f"images/fig.{i}.png")


def make_barchart(df: pd.DataFrame):
    fig = px.bar(df,
                 x=df.index.get_level_values("client_count"),
                 y="RecordsPerSecond",
                 color=df.index.get_level_values("plain_or_pbac"),
                 title="",
                 barmode="group",
                 labels=dict(x="#Clients", color="Mode"))
    fig.update_xaxes(type='category')

    fig.show()


def bla(start_ts: str, client_count: int, plain_or_pbac: str, produce_or_consume: str):
    means = []
    dfs = read_df(i)
    means.append({
        "RecordsPerSecond": dfs["pbac"]["RecordsPerSecond"].mean(),
        "ProducerCount": str(i),
        "System Under Test": "PBAC"
    })
    means.append({
        "RecordsPerSecond": dfs["plain"]["RecordsPerSecond"].mean(),
        "ProducerCount": str(i),
        "System Under Test": "Plain"
    })

    fig = px.bar(means, x="ProducerCount", y="RecordsPerSecond", color="System Under Test",
                 title="Throughput by #Producers", barmode="group")
    fig.show()

    if not os.path.exists("images"):
        os.mkdir("images")

    fig.write_image(f"images/fig.png")


if __name__ == "__main__":
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

        file_name = f"results/{run.get('produce_or_consume')}.{run.get('plain_or_pbac')}.{client_count}.{file_format_begin_ts}.csv"

        if not os.path.exists(file_name):
            get_timeseries_from_logs(run.get('begin_ts'), run.get('end_ts'), file_name)

        df = pd.read_csv(file_name, parse_dates=["Timestamp"])
        df["plain_or_pbac"] = plain_or_pbac
        df["produce_or_consume"] = produce_or_consume
        df["seconds_since_start"] = (df["Timestamp"] - maxes.loc[client_count]["begin_ts"]).map(
            lambda x: x.total_seconds())
        df["client_count"] = client_count

        # make_scatterplot(client_count, df)

        all_data = pd.concat((all_data, df))

    avg_sum_throughput = all_data.groupby(["plain_or_pbac", "produce_or_consume", "client_count"]).mean()[
        "RecordsPerSecond"].multiply(list(maxes.T)).to_frame("RecordsPerSecond")

    make_barchart(avg_sum_throughput)

    all_data.sum()
    pass
