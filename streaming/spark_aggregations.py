from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, current_timestamp

POSTGRES_URL = "jdbc:postgresql://postgres:5432/github_analytics"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def main():
    spark = (
        SparkSession.builder
        .appName("GitHubAggregations")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ===============================
    # Load fact table
    # ===============================
    events_df = (
        spark.read
        .jdbc(
            url=POSTGRES_URL,
            table="github_events_fact",
            properties=POSTGRES_PROPERTIES
        )
    )

    # ===============================
    # 1️⃣ Events per minute
    # ===============================
    events_minute = (
        events_df
        .groupBy(
            window(col("event_created_at"), "1 minute")
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("minute"),
            col("event_count")
        )
    )

    events_minute.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_events_minute",
            properties=POSTGRES_PROPERTIES
        )

    # ===============================
    # 2️⃣ Event types
    # ===============================
    event_types = (
        events_df
        .groupBy("event_type")
        .agg(count("*").alias("event_count"))
        .withColumn("last_updated", current_timestamp())
    )

    event_types.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_event_types",
            properties=POSTGRES_PROPERTIES
        )

    # ===============================
    # 3️⃣ Repo leaderboard
    # ===============================
    repo_activity = (
        events_df
        .groupBy("repo_name")
        .agg(count("*").alias("event_count"))
        .withColumn("last_updated", current_timestamp())
    )

    repo_activity.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_repo_activity",
            properties=POSTGRES_PROPERTIES
        )

    # ===============================
    # 4️⃣ Actor leaderboard
    # ===============================
    actor_activity = (
        events_df
        .groupBy("actor_login")
        .agg(count("*").alias("event_count"))
        .withColumn("last_updated", current_timestamp())
    )

    actor_activity.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_actor_activity",
            properties=POSTGRES_PROPERTIES
        )

    spark.stop()

if __name__ == "__main__":
    main()