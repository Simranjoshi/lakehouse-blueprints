"""Land raw files into a bronze Delta table with a locked schema."""

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType


BRONZE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("entity_id", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("source_system", StringType(), False),
        StructField("payload_json", StringType(), True),
        StructField("ingest_batch_id", LongType(), False),
    ]
)


def bronze_reader(spark: SparkSession, path: str):
    return (
        spark.read.format("json")
        .schema(BRONZE_SCHEMA)
        .option("mode", "FAILFAST")
        .load(path)
    )


def write_bronze(df, table_path: str) -> None:
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .partitionBy("source_system")
        .save(table_path)
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("bronze-ingest").getOrCreate()
    print("Define source path and table path before running in a workspace.")
    spark.stop()
