"""Build data/taxi_sample.csv from NYC yellow taxi January 2024.

The producer is the only process allowed to read this file. Download is
skipped when the parquet or the csv is already present.
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / "data" / "yellow_tripdata_2024-01.parquet"
CSV = ROOT / "data" / "taxi_sample.csv"
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
CAP = 40000


def ensure_parquet() -> None:
    if PARQUET.exists() and PARQUET.stat().st_size > 1_000_000:
        return
    PARQUET.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {URL}")
    urllib.request.urlretrieve(URL, PARQUET)


def main() -> None:
    if CSV.exists() and "--force" not in sys.argv:
        print(f"sample already exists: {CSV} ({sum(1 for _ in open(CSV, encoding='utf-8')) - 1} rows)")
        return
    ensure_parquet()
    columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "passenger_count",
        "payment_type",
    ]
    frame = pd.read_parquet(PARQUET, columns=columns)
    frame = frame.rename(columns={
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    })
    frame = frame.dropna(subset=["pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID"])
    frame = frame[(frame["PULocationID"] > 0) & (frame["DOLocationID"] > 0)]
    frame = frame[(frame["pickup_datetime"] >= "2024-01-01") & (frame["pickup_datetime"] < "2024-02-01")]
    frame = frame[(frame["dropoff_datetime"] >= "2024-01-01") & (frame["dropoff_datetime"] < "2024-02-02")]
    frame["hour"] = frame["pickup_datetime"].dt.floor("h")
    counts = frame.groupby("hour").size().sort_index()
    rolling = counts.rolling(4).sum()
    end = rolling.idxmax()
    start = end - pd.Timedelta(hours=3)
    window = frame[(frame["pickup_datetime"] >= start) & (frame["pickup_datetime"] < end + pd.Timedelta(hours=1))]
    window = window.sort_values("pickup_datetime")
    window = window.drop(columns=["hour"]).head(CAP)
    CSV.parent.mkdir(parents=True, exist_ok=True)
    window.to_csv(CSV, index=False)
    print(
        f"wrote {len(window)} rows {window['pickup_datetime'].iloc[0]} -> "
        f"{window['pickup_datetime'].iloc[-1]} from the busiest 4-hour block starting {start}"
    )


if __name__ == "__main__":
    main()
