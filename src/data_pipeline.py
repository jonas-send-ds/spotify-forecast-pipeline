
import polars as pl

from src.feature_engineering import add_features
from src.songstats_data import load_songstats_data
# from src.spreadsheet_data import load_spreadsheet_data

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
        - df_training: processed dataset for training, with null values removed
        - df_latest: dataset containing the most recent data for each artist
    """
    # df_spreadsheet = load_spreadsheet_data(ARTISTS.keys())  # TODO #11: is spreadsheet data sufficiently more up-to-date to justify additional complexity?
    df = load_songstats_data(ARTISTS).select(["date", "artist", "monthly_listeners", "reach"])
    df = add_features(df)

    df_training = df.drop_nulls()
    df_latest = (df
                 .sort("date")
                 .with_columns(pl.all().fill_null(strategy="forward").over("artist"))
                 .group_by("artist")
                 .last())

    print(f"Training data available from {df_training["date"].min()} to {df_training['date'].max()} for {len(df_training["artist"].unique())} artists")
    print(f"Latest data available from {df_latest['date'].min()} to {df_latest['date'].max()} for {len(df_latest)} artists")

    return df_training, df_latest
