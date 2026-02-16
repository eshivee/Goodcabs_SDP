from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name = 'goodcabs.silver.city',
    comment = 'Silver ingestion for city table',
    table_properties = {
        'quality' : 'silver',
        'layer' : 'silver',
        'delta.enableChangeDataFeed' : 'true',
        'delta.autoOptimize.optimizeWrite' : 'true',
        'delta.autoOptimize.autoCompact' : 'true'
    }
)
def city_silver():
    df_bronze = spark.read.table('goodcabs.bronze.city')
    df_silver = df_bronze.select(
        col('city_id'), 
        col('city_name'), 
        col('time_ingested').alias('bronze_time_ingested')
    )
    df_silver = df_silver.withColumn('silver_time_ingested', current_timestamp())
    return df_silver