#Configurations

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2


@dp.materialized_view(
    name="poc_project.conformed.city",
    comment="Conformed city data with standardized columns and hash key",
    cluster_by_auto=True,
    table_properties={
        "source_format":"true",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name"
    }
)
def conformed_city():
    df_raw = spark.read.table("poc_project.raw.city")
    
    df_conformed = df_raw.withColumnRenamed("ingest_datetime", "raw_ingestion_datetime").withColumn("conformed_datetime", current_timestamp())

    return df_conformed
