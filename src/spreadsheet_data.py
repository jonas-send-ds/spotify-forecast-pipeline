"""Module for loading and processing data from a Google Spreadsheet."""

import json
import os
from pathlib import Path
from typing import Iterable

import gspread
import polars as pl
from dotenv import load_dotenv

PRIVATE_KEY_IS_NOT_SET = "GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY environment variable is not set"
SPREADSHEET_ID = "167UTVu2XVAM0MlGw-Cpw0tcMyuphC3ifJpOiC_y_a74"


def get_credentials() -> dict:
    """
    Retrieve the Google service account credentials.

    :return: A dictionary containing the credentials-data.
    """
    print("Loading Google Service Account credentials...")
    with Path.open(Path("google-service-account.json")) as file:
        credentials = json.load(file)

    load_dotenv()
    private_key_env = os.getenv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY")

    if not private_key_env:
        raise ValueError(PRIVATE_KEY_IS_NOT_SET)

    # HACKY: Replace literal \n with actual newline characters
    credentials["private_key"] = private_key_env.replace("\\n", "\n")

    return credentials


def load_spreadsheet_data(artists: Iterable[str]) -> pl.DataFrame:
    """
    Loads and processes data from a Google Spreadsheet containing monthly listener
    statistics for specified artists.

    :param artists: A list of artist names to select specific columns from the spreadsheet.
    :return: A Polars DataFrame containing the processed data with the following
        columns: date, artist, and monthly_listeners. The DataFrame is sorted by
        date and artist, with missing values removed.
    """
    credentials = get_credentials()
    client = gspread.service_account_from_dict(credentials)

    print("Loading and processing data from Google Spreadsheet...")
    public_sheet: gspread.Spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = public_sheet.worksheet("Monthly Listeners")
    data = worksheet.get_all_values()  # list of lists

    df = pl.DataFrame(
        data[1:],
        schema=data[0],  # first row as column names
        orient="row",
    )

    return (df.select(["Date", *[x for x in artists]])
            .rename({"Date": "date"})
            .unpivot(index="date", variable_name="artist", value_name="monthly_listeners")
            .with_columns(
                pl.col("date").str.to_date("%m/%d/%Y"),
                pl.col("monthly_listeners").str.replace_all(",", "").replace("", None).cast(pl.Int64))
            .drop_nulls()
            .sort(["date", "artist"]))
