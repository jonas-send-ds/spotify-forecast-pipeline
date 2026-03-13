"""Add features for model training."""

import polars as pl

LAG_DAYS = [7, 14, 21, 28, 56, 168]
MA_DAYS = [(1, 7), (7, 28), (28, 84), (28, 168)]


def add_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add engineered features to the DataFrame.

    :param df: DataFrame to add features to.
    :return: DataFrame with engineered features.
    """
    print("Adding features...")

    pct_change_expressions = [
        pl.col("reach").pct_change(days).over("artist").alias(f"reach_pct_change_{days}")
        for days in LAG_DAYS
    ]
    moving_average_expressions = [
        (pl.col("monthly_listeners").rolling_mean(short_days) / pl.col("monthly_listeners").rolling_mean(long_days)).over("artist").alias(
            f"monthly_listeners_ma_{short_days}_over_{long_days}")
        for short_days, long_days in MA_DAYS
    ]

    return df.with_columns(pct_change_expressions).with_columns(moving_average_expressions)
