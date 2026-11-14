"""Conformed silver grain: latest record per entity, typed, de-duplicated."""

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, to_timestamp


def to_silver(bronze_df):
    window = Window.partitionBy("entity_id").orderBy(col("event_ts").desc())
    return (
        bronze_df.withColumn("event_ts", to_timestamp("event_ts"))
        .withColumn("rank", row_number().over(window))
        .where(col("rank") == 1)
        .drop("rank", "payload_json")
    )
