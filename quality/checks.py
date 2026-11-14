"""Fail the job when grain, nulls, or freshness break."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def require_unique(df: DataFrame, columns: list[str], label: str) -> None:
    dupes = df.groupBy(*columns).count().filter(F.col("count") > 1).limit(1)
    if dupes.take(1):
        raise DataQualityError(f"{label}: duplicate grain on {columns}")


def require_non_null(df: DataFrame, columns: list[str], label: str) -> None:
    for column in columns:
        if df.filter(F.col(column).isNull()).limit(1).take(1):
            raise DataQualityError(f"{label}: nulls in {column}")


def require_fresh(df: DataFrame, ts_col: str, hours: int, label: str) -> None:
    if not df.limit(1).take(1):
        raise DataQualityError(f"{label}: empty table")
    stale = df.select(F.max(ts_col).alias("max_ts")).filter(
        F.col("max_ts") < F.expr(f"current_timestamp() - interval {hours} hours")
    )
    if stale.take(1):
        raise DataQualityError(f"{label}: last event older than {hours} hours")
