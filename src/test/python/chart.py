import plotly.express as px
import pandas as pd
import os
import plotly.io as pio
from get_logs import get_timeseries_from_logs

from pandas import DataFrame

pio.kaleido.scope.default_scale = 8


def make_scatterplot(i, df):
    return px.scatter(df, x="seconds_since_start", y="RecordsPerSecond", color="plain_or_pbac", trendline="lowess",
                      title=f"Throughput over time, {i} producers",
                      labels={"RecordsPerSecond": "Throughput [Records/s]",
                              "seconds_since_start": "Time elapsed [s]",
                              "plain_or_pbac": "System under test"})


def make_barchart(df: pd.DataFrame):
    fig = px.bar(df,
                 x=df.index.get_level_values("client_count"),
                 y="RecordsPerSecond",
                 color=df.index.get_level_values("plain_or_pbac"),
                 title="Average Throughput",
                 barmode="group",
                 labels=dict(x="#Clients", color="System under Test"))
    fig.update_xaxes(type='category')
    return fig


def check_consistency(df: pd.DataFrame):
    measurement_points_by_producer = df.groupby("Producer").size()
    reference_num_measurement_points = measurement_points_by_producer.iloc[0]
    allowed_abs_deviation = reference_num_measurement_points // 100
    deviating_num_measurement_points_reported = ((measurement_points_by_producer - reference_num_measurement_points).abs() > allowed_abs_deviation).any()
    if deviating_num_measurement_points_reported:
        raise AssertionError("Got unequal number of measurement points: " + str(measurement_points_by_producer))


if __name__ == "__main__":
    if not os.path.exists("images"):
        os.mkdir("images")

    runs: DataFrame = pd.read_csv("BenchmarkRuns.csv", sep=";", parse_dates=["begin_ts", "end_ts"],
                                  names=("produce_or_consume", "plain_or_pbac", "client_count", "begin_ts", "end_ts"))

    group_for_latest_run = ["plain_or_pbac", "produce_or_consume", "client_count"]
    latest_runs = runs.groupby(group_for_latest_run).max()

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

        check_consistency(df)

        image_file_name = f"images/{file_name}.png"
        if not os.path.exists(image_file_name):
            fig = make_scatterplot(client_count, df)
            fig.write_image(image_file_name)

        all_data = pd.concat((all_data, df))

    for (produce_or_consume, client_count), group in all_data.groupby(["produce_or_consume", "client_count"]):
        can_compare_plain_and_pbac = 'PBAC' in group['plain_or_pbac'].values and \
                                     'Plain' in group['plain_or_pbac'].values
        if can_compare_plain_and_pbac:
            image_file_name = f"images/{produce_or_consume}.Compare.{client_count}.png"
            fig = make_scatterplot(client_count, group)
            fig.write_image(image_file_name)

    avg_throughput = all_data \
        .query("seconds_since_start > 400") \
        .groupby(group_for_latest_run) \
        .mean() \
        ["RecordsPerSecond"]
    avg_sum_throughput = avg_throughput \
        .multiply(avg_throughput.index.get_level_values("client_count")) \
        .to_frame("RecordsPerSecond")

    fig = make_barchart(avg_sum_throughput)
    fig.write_image(f"images/SumThroughput.png")
