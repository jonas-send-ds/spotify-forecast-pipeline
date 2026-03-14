
import os
import requests

import polars as pl
from dotenv import load_dotenv
from tqdm import tqdm

API_URL = "https://api.songstats.com/enterprise/v1/artists/historic_stats"


def load_songstats_data(artists: dict[str, str]) -> pl.DataFrame:
    load_dotenv()

    print("Loading songstats data...")
    df_list = []
    for artist, songstats_id in tqdm(artists.items()):
        response = requests.get(
            API_URL,
            headers={"apikey": os.getenv("SONGSTATS_API_KEY")},
            params={
                "songstats_artist_id": songstats_id,
                "source": "spotify",
                "with_aggregates": "true",
                "start_date": "2020-06-01"  # before that the API behaves funky with respect to reach data
            }
        )

        df= pl.DataFrame(response.json()["stats"][0]["data"]["history"])
        df = df.with_columns(
            pl.col("date").str.to_date("%Y-%m-%d"),
            pl.lit(artist).alias("artist")
        )

        df_list.append(df)

    df = (pl.concat(df_list)
            .rename({
                "monthly_listeners_current": "monthly_listeners",
                "playlists_current": "playlists",
                "playlist_reach_current": "reach"
            })
            .sort(["date", "artist"])
            .filter(pl.col("monthly_listeners") > 0))

    df = fix_anomalies(df)

    # We interpret the monthly listeners values as lagged by one day
    return df.with_columns(
        pl.col("monthly_listeners").shift(-1).over("artist")
    )


def fix_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Manually fix data anomalies.

    :param df: Dataframe to fix anomalies in
    :return: Dataframe with anomalies fixed
    """
    anomaly_mask = (((pl.col("artist") == "Bruno Mars") & (pl.col("date").is_between(pl.date(2026, 2, 15), pl.date(2026, 2, 16)))) |
                    ((pl.col("artist") == "Bad Bunny") & (pl.col("date") == pl.date(2021, 2, 16))))
    df = df.with_columns(
        pl.when(anomaly_mask).then(None).otherwise(pl.col("playlists")).alias("playlists"),
        pl.when(anomaly_mask).then(None).otherwise(pl.col("reach")).alias("reach"),
    )
    # Linear interpolation
    numeric_columns = [column for column, dtype in df.schema.items() if dtype.is_numeric()]
    return df.with_columns(
        pl.col(column).interpolate().over("artist")
        for column in numeric_columns
    )
