import plotly.express as px
import pandas as pd
import os
import plotly.io as pio
from get_logs import get_timeseries_from_logs

from pandas import DataFrame

pio.kaleido.scope.default_scale = 8


def make_scatterplot(title, df):
    return px.scatter(df, x="seconds_since_start", y="RecordsPerSecond", color="plain_or_pbac", trendline="lowess",
                      title=title,
                      labels={"RecordsPerSecond": "Throughput [Messages/s]",
                              "seconds_since_start": "Time elapsed [s]",
                              "plain_or_pbac": "System under test"})


def make_barchart(df: pd.DataFrame):
    df = df.reset_index()

    fig = px.bar(df,
                 x="client_count",
                 y="RecordsPerSecond",
                 color="plain_or_pbac",
                 facet_row="produce_or_consume",
                 title="Average Throughput Of All Clients",
                 barmode="group",
                 category_orders={'produce_or_consume': ['Produce', 'Consume']},
                 labels=dict(client_count="#Clients", plain_or_pbac="System under Test",
                             RecordsPerSecond="Throughput [Messages/s]"),
                 width=800,
                 height=800)
    fig.update_xaxes(type='category')  # don't show unused client counts
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))  # set subtitles to Consume / Produce
    fig.update_yaxes(matches=None)
    return fig


def check_consistency(df: pd.DataFrame):
    measurement_points_by_producer = df.groupby("Producer").size()
    reference_num_measurement_points = measurement_points_by_producer.iloc[0]
    allowed_abs_deviation = max(1, reference_num_measurement_points // 100)
    deviating_num_measurement_points_reported = (
                (measurement_points_by_producer - reference_num_measurement_points).abs() > allowed_abs_deviation).any()
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
        print("Processing " + str(runtuple))
        begin_ts = runtuple.begin_ts
        end_ts = runtuple.end_ts
        plain_or_pbac, produce_or_consume, client_count = runtuple.Index

        if client_count > 8:
            print("Skipping ")
            continue

        file_format_begin_ts = begin_ts.strftime("%Y%m%dT%H%M%S%z")

        begin_ts = begin_ts.isoformat()
        end_ts = end_ts.isoformat()

        file_name = f"{produce_or_consume}.{plain_or_pbac}.{client_count}.{file_format_begin_ts}"
        csv_file_name = f"results/{file_name}.csv"

        if not os.path.exists(csv_file_name):
            print("Getting logs...")
            get_timeseries_from_logs(produce_or_consume, begin_ts, end_ts, csv_file_name)

        df = pd.read_csv(csv_file_name, parse_dates=["Timestamp"])
        if not len(df):
            print("No data found in " + csv_file_name)
            continue

        df["plain_or_pbac"] = plain_or_pbac
        df["produce_or_consume"] = produce_or_consume
        df["seconds_since_start"] = (df["Timestamp"] - runtuple.begin_ts).map(lambda x: x.total_seconds())
        df["client_count"] = client_count

        check_consistency(df)

        image_file_name = f"images/{file_name}.png"
        if not os.path.exists(image_file_name):
            fig = make_scatterplot(f"Throughput Over Time, {client_count} {produce_or_consume}rs, {plain_or_pbac}", df)
            fig.write_image(image_file_name)

        all_data = pd.concat((all_data, df))

    for (produce_or_consume, client_count), group in all_data.groupby(["produce_or_consume", "client_count"]):
        can_compare_plain_and_pbac = 'PBAC' in group['plain_or_pbac'].values and \
                                     'Plain' in group['plain_or_pbac'].values
        if can_compare_plain_and_pbac:
            image_file_name = f"images/{produce_or_consume}.Compare.{client_count}.png"
            fig = make_scatterplot(f"Throughput Over Time, {client_count} {produce_or_consume}rs", group)
            fig.write_image(image_file_name)

    avg_throughput = all_data \
        .query("seconds_since_start > 5") \
        .groupby(group_for_latest_run) \
        .mean() \
        ["RecordsPerSecond"]
    avg_sum_throughput = avg_throughput \
        .multiply(avg_throughput.index.get_level_values("client_count")) \
        .to_frame("RecordsPerSecond")

    fig = make_barchart(avg_sum_throughput)
    fig.write_image(f"images/SumThroughput.png")
