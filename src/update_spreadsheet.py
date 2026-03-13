
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import gspread
import polars as pl
from dotenv import load_dotenv
from gspread.utils import ValueInputOption

PRIVATE_KEY_IS_NOT_SET = "GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY environment variable is not set"
SPREADSHEET_NAME_IS_NOT_SET = "GOOGLE_SPREADSHEET_NAME environment variable is not set"


def get_credentials() -> dict:
    """
    Retrieve the Google service account credentials.

    :return: A dictionary containing the credentials data.
    """
    with Path.open(Path("google-service-account.json")) as file:
        credentials = json.load(file)

    load_dotenv()
    private_key_env = os.getenv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY")

    if not private_key_env:
        raise ValueError(PRIVATE_KEY_IS_NOT_SET)

    # HACKY: Replace literal \n with actual newline characters
    credentials["private_key"] = private_key_env.replace("\\n", "\n")

    return credentials


def update_spreadsheet(df_forecast: pl.DataFrame) -> None:
    """
    Update the Google Spreadsheet with the provided data.

    :param df_forecast: A dataframe containing the forecast data.

    :return: None
    """
    print("Fetching spreadsheet...")
    credentials = get_credentials()
    gc = gspread.service_account_from_dict(credentials)

    load_dotenv()
    spreadsheet_name = os.getenv("GOOGLE_SPREADSHEET_NAME")

    if not spreadsheet_name:
        raise ValueError(SPREADSHEET_NAME_IS_NOT_SET)

    spreadsheet = gc.open(spreadsheet_name)

    datetime_now = datetime.now(ZoneInfo("America/Chicago"))

    df_forecast = df_forecast.sort("artist")

    worksheet_forecast = spreadsheet.worksheet("Forecast")

    print("Updating spreadsheet 'Forecast'...")
    for row_index in range(len(df_forecast)):
        worksheet_forecast.update_acell(f"A{2 + row_index}", df_forecast["artist"][row_index])
        worksheet_forecast.update_acell(f"B{2 + row_index}", f"{df_forecast['monthly_listeners'][row_index]:,}")
        worksheet_forecast.update_acell(f"C{2 + row_index}", f"{df_forecast['forecast'][row_index]:,}")


    worksheet_forecast.update_acell("E2", df_forecast["forecast_date"][0].strftime("%Y-%m-%d"))
    tz_abbrev = datetime_now.strftime("%Z")
    timestamp_ct = datetime_now.strftime(f"%Y-%m-%d %H:%M:%S {tz_abbrev}")
    worksheet_forecast.update_acell("G2", timestamp_ct)

    worksheet_history = spreadsheet.worksheet("History")

    print("Updating spreadsheet 'History'...")
    for row_index in range(len(df_forecast) - 1, -1, -1):  # fill backwards
        worksheet_history.insert_row(values=[
            timestamp_ct,
            df_forecast["artist"][row_index],
            df_forecast["latest_date"][row_index].strftime("%Y-%m-%d"),
            f"{df_forecast['monthly_listeners'][row_index]:,}",
            df_forecast["forecast_date"][row_index].strftime("%Y-%m-%d"),
            f"{df_forecast['forecast'][row_index]:,}",
        ],
            index=2,
            value_input_option=ValueInputOption.user_entered  # this improves formatting
        )
