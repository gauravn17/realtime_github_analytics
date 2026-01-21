# enrich_metadata.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col


def enrich_repo_metadata(df: DataFrame) -> DataFrame:
    """
    Enrich GitHub events with derived repository metadata.
    Safe to call inside Spark streaming jobs.
    """

    return (
        df
        .withColumn("repo_owner", split(col("repo_name"), "/").getItem(0))
        .withColumn("repo_short_name", split(col("repo_name"), "/").getItem(1))
    )
