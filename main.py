"""The project's main module."""

from src.data_pipeline import load_data
from src.forecast import forecast
from src.s3_util import persist_latest_data_to_s3
from src.update_spreadsheet import update_spreadsheet


def main() -> None:
    """
    Execute the main application workflow.

    :return: None
    """
    df_train, df_latest = load_data()
    df_forecast = forecast(df_train, df_latest)
    update_spreadsheet(df_forecast)
    persist_latest_data_to_s3(df_latest)

    print("Done!")


if __name__ == "__main__":
    main()
