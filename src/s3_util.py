
import io
import os

import boto3
import polars as pl
from dotenv import load_dotenv

S3_FILE_KEY = "latest_data.parquet"
VARIABLE_IS_NOT_SET = "S3_BUCKET_NAME environment variable is not set"


def persist_latest_data_to_s3(df_latest: pl.DataFrame) -> None:
    """
    Persist the latest forecast data to an S3 bucket as a Parquet file.

    :param df_latest: DataFrame containing the latest data to persist.
    :raises ValueError: If the S3_BUCKET_NAME environment variable is not set.
    """
    load_dotenv()
    bucket = os.getenv("S3_BUCKET_NAME")
    if bucket is None:
        raise ValueError(VARIABLE_IS_NOT_SET)

    df_latest = df_latest.select(["date", "artist", "monthly_listeners", "reach"])

    buffer = io.BytesIO()
    df_latest.write_parquet(buffer)
    buffer.seek(0)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=S3_FILE_KEY,
        Body=buffer.getvalue(),
    )