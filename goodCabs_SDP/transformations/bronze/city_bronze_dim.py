from pyspark.sql.functions import *
from pyspark import pipelines as dp
source_path = 's3://goodcabs-transport-sdp/1. data/city/'

@dp.materialized_view(
    name = 'goodcabs.bronze.city',
    comment = 'Bronze ingestion for city table',
    table_properties = {
        'quality' : 'bronze',
        'layer' : 'bronze',
        'source_format' : 'csv',
        'delta.enableChangeDataFeed' : 'true',
        'delta.autoOptimize.optimizeWrite' : 'true',
        'delta.autoOptimize.autoCompact' : 'true'
    }
)
def city_table():
    df = spark.read.format('csv')\
        .option('header', True)\
        .option('inferSchema', True)\
        .option('mode', 'PERMISSIVE')\
        .option('mergeSchema', True)\
        .option('columnNameOfCorruptRecord', 'corrupt_records')\
        .load(source_path)

    df = df.withColumn('file_name', col('_metadata.file_path'))\
            .withColumn('time_ingested', current_timestamp())
    return df
