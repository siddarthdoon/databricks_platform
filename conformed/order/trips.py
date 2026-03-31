from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.view(
    name="trips_conformed_staging", comment="Transformed trips data ready for CDC upsert"
)
@dp.expect("valid_date", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")

def trips_conformed():
    df_raw = spark.readStream.table("poc_project.raw.trips")
    df_conformed = df_raw.withColumn("passenger_type", F.lower("passenger_type"))

    df_conformed = df_raw.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("raw_ingest_timestamp"),
    )

    df_conformed = df_conformed.withColumn(
        "conformed_processed_timestamp", F.current_timestamp()
    )
    return df_conformed


dp.create_streaming_table(
    name="poc_project.conformed.trips",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "conformed",
        "layer": "conformed",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    target="poc_project.conformed.trips",
    source="trips_conformed_staging",
    keys=["id"],
    sequence_by=F.col("conformed_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
