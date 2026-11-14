"""Gold mart: prune by date, keep only consumption columns."""

from pyspark.sql.functions import col, to_date


def to_gold(silver_df):
    return (
        silver_df.withColumn("event_date", to_date("event_ts"))
        .select("entity_id", "source_system", "event_date", "event_ts", "ingest_batch_id")
        .where(col("entity_id").isNotNull())
    )
