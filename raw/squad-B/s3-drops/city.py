#Configurations

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2


s3_path = "s3://projectpoc-dbpipeline1/datastore/city/"


@dp.materialized_view(
    name="poc_project.raw.city",
    comment="Raw city data ingested from S3 CSV files",
    cluster_by_auto=True,
    table_properties={
        "source_format":"true",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.columnMapping.mode": "name"
    }
)
def city_raw():
    df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mergeSchema","true") \
    .option("mode","PERMISSIVE") \
    .option("ColumnNameOfCorruptRecords","_corrupt_record") \
    .load(s3_path)


    df= df.withColumn("file_name",col("_metadata.file_path")).withColumn("ingest_datetime",current_timestamp())

    return df
