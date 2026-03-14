
import polars as pl

from src.feature_engineering import add_features
from src.songstats_data import load_songstats_data
from src.spreadsheet_data import load_spreadsheet_data

# artists with their songstats ID
ARTISTS = {
    "Bad Bunny": "xmcd3klh",
    "Bruno Mars": "2j0zuon6",
    "The Weeknd": "93b216mv",
}

def load_data() -> (pl.DataFrame, pl.DataFrame):
    """
    Loads and processes song statistics data, and returns two datasets:
    a training dataset with null values removed, and a latest dataset
    containing the most recent data for each artist.

    :return: A tuple containing two datasets:
        - df_train: processed dataset for training, with null values removed
        - df_latest: dataset containing the most recent data for each artist
    """
    df_songstats = load_songstats_data(ARTISTS).select(["date", "artist", "monthly_listeners", "reach"])
    df_spreadsheet = load_spreadsheet_data(ARTISTS.keys())

    df = pl.concat([df_songstats, df_spreadsheet], how="diagonal_relaxed")

    # use monthly listeners data from spreadsheet (when available) and songstats data for reach
    df = (df.group_by(["date", "artist"]).agg(
              pl.col("monthly_listeners").last(),
              pl.col("reach").first(),
          ).sort(["date", "artist"]))


    df = add_features(df)

    df_train = df.drop_nulls()
    df_latest = (df
                 .sort("date")
                 .with_columns(pl.all().fill_null(strategy="forward").over("artist"))
                 .group_by("artist")
                 .last())

    print(f"Training data available from {df_train["date"].min()} to {df_train['date'].max()} for {len(df_train["artist"].unique())} artists")
    print(f"Latest data available from {df_latest['date'].min()} to {df_latest['date'].max()} for {len(df_latest)} artists")

    return df_train, df_latest
