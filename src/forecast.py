
import calendar
from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
from sklearn.linear_model import LinearRegression

SELECTED_FEATURES = ["monthly_listeners_ma_1_over_7",
    "monthly_listeners_ma_7_over_28",
    "monthly_listeners_ma_28_over_84",
    "monthly_listeners_ma_28_over_168",
    "reach_pct_change_7",
    "reach_pct_change_14",
    "reach_pct_change_21",
    "reach_pct_change_28",
    "reach_pct_change_56",
    "reach_pct_change_168",
]


def get_length_of_month(day: date) -> int:
    """
    Determine the number of days in the month for a given date.

    :param day: The date object for which to calculate the number of days in the corresponding month.
    :return: The number of days in the month of the provided date.
    """
    return calendar.monthrange(day.year, day.month)[1]


def forecast(df_train: pl.DataFrame, df_latest: pl.DataFrame):
    """
    Forecasts future values using trained models based on the given training
    and latest data. The function determines the forecasting horizon dynamically
    based on the latest dates in the provided data and the last day of the current
    month. Models are trained for these horizons, and forecasts are then generated.

    :param df_train: Training dataset used to fit the forecasting models
    :param df_latest: Latest dataset containing the most recent data for forecasting
    :return: Forecasts generated for the given horizons as a Polars DataFrame
    """
    today = datetime.now(ZoneInfo("America/Chicago")).date()
    last_day_of_current_month = today.replace(day=get_length_of_month(today))

    latest_dates = df_latest["date"].unique().to_list()
    horizons = []
    for latest_date in latest_dates:
        horizons.append((last_day_of_current_month - latest_date).days)

    models = train_models(df_train, horizons)

    return generate_forecasts(df_latest, last_day_of_current_month, models)


def train_models(df_train: pl.DataFrame, horizons: list[int]) -> dict[int, LinearRegression]:
    """
    Fits linear regression models for given prediction horizons based on training data.

    :param df_train: The training dataset containing features and target column
        "monthly_listeners".
    :param horizons: A list of prediction horizons for which models should be trained.
    :return: A dictionary where keys represent prediction horizons and values are
        the trained sklearn LinearRegression models for each respective horizon.
    """
    print("Fitting models...")
    models = dict()
    for horizon in horizons:
        df_horizon = df_train.with_columns(
            pl.col("monthly_listeners").pct_change(horizon).shift(-horizon).over("artist").alias(f"growth_target_{horizon}")
        )
        df_horizon = df_horizon.drop_nulls()

        linear_model = LinearRegression(fit_intercept=True)
        linear_model.fit(df_horizon[SELECTED_FEATURES], df_horizon[f"growth_target_{horizon}"])

        models[horizon] = linear_model

    return models


def generate_forecasts(df_latest: pl.DataFrame, last_day_of_current_month: date, models: dict[int, LinearRegression]) -> pl.DataFrame:
    """
    Generates forecasts for each artist using the given data and models, producing a DataFrame
    containing the forecasts.

    :param df_latest: A DataFrame containing the latest data for each artist, including features that
        are used for prediction, grouped by 'artist'.
    :param last_day_of_current_month: The last date of the current month, which serves as the
        forecast target date.
    :param models: A dictionary mapping the prediction horizon (in days) to instances of
        LinearRegression models used to predict growth.
    """
    print("Generating forecasts...")
    df_list = []
    for artist, row in df_latest.group_by("artist"):
        latest_date = row["date"].item()
        horizon = (last_day_of_current_month - latest_date).days
        growth_forecast = models[horizon].predict(row[SELECTED_FEATURES])[0]
        monthly_listeners = row["monthly_listeners"]
        forecast = monthly_listeners * (1 + growth_forecast)
        df_loop = pl.DataFrame({
            "artist": artist,
            "latest_date": latest_date,
            "monthly_listeners": monthly_listeners,
            "forecast_date": last_day_of_current_month,
            "forecast": forecast,
        })
        df_list.append(df_loop)

    return pl.concat(df_list)
