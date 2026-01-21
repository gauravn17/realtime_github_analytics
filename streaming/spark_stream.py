from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "github_events"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/github-events"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/github_analytics"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url=POSTGRES_URL,
            table="github_events_fact",
            properties=POSTGRES_PROPERTIES
        )
    )
def main():
    spark = (
        SparkSession.builder
        .appName("GitHubKafkaStream")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    json_df = raw_df.select(
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("json")
    )

    event_schema = StructType([
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        StructField("actor", StructType([
            StructField("id", LongType()),
            StructField("login", StringType())
        ])),
        StructField("repo", StructType([
            StructField("id", LongType()),
            StructField("name", StringType())
        ]))
    ])

    parsed_df = json_df.select(
        from_json(col("json"), event_schema).alias("event"),
        "partition",
        "offset",
        "kafka_timestamp"
    )

    flattened_df = parsed_df.select(
        col("event.id").alias("event_id"),
        col("event.type").alias("event_type"),
        to_timestamp(col("event.created_at")).alias("event_created_at"),
        col("event.actor.id").alias("actor_id"),
        col("event.actor.login").alias("actor_login"),
        col("event.repo.id").alias("repo_id"),
        col("event.repo.name").alias("repo_name"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp")
    )

    query = (
        flattened_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()