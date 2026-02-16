from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.view(
    name = 'trips_silver_stg',
    comment = 'transformation layer for trips from the bronze layer'
)

def silver_trips_stg_v():
    df = spark.readStream.table('goodcabs.bronze.trips')
    df_silver = df.withColumn('passenger_type', initcap('passenger_type'))

    df_silver = df_silver.select(col("trip_id").alias("id"),
        col("date").cast("date").alias("business_date"),
        col("city_id"),
        col("passenger_type").alias("passenger_category"),
        col("distance_travelled_km").alias("distance_kms"),
        col("fare_amount"),
        col("passenger_rating"),
        col("driver_rating"),
        col("bronze_ingested_time")

    )

    df_silver = df_silver.withColumn('silver_ingested_timestamp', current_timestamp())

    return df_silver

dp.create_streaming_table(
    name = 'goodcabs.silver.trips',
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties= {
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

dp.create_auto_cdc_flow(
    target="goodcabs.silver.trips",
    source="trips_silver_stg",
    keys=["id"],
    sequence_by=col("silver_ingested_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
