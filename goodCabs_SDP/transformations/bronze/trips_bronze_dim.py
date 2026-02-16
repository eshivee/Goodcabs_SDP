from pyspark import pipelines as dp
from pyspark.sql.functions import *

source_url ='s3://goodcabs-transport-sdp/1. data/trips/Full Load/'

@dp.table(
    name = 'goodcabs.bronze.trips',
    comment = 'Bronze ingestion for trips table',
    table_properties = {
        'quality' : 'bronze',
        'layer' : 'bronze',
        'delta.enableChangeDataFeed' : 'true',
        'delta.autoOptimize.optimizeWrite' : 'true',
        'delta.autoOptimize.autoCompact' : 'true'
    }
)
       
def trips_bronze():
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
        .option('cloudFiles.schemaEvolutionMode', 'rescue')\
        .option('cloudFiles.maxFilesPerTrigger', 100)\
        .option('cloudFiles.inferColumnTypes', 'true')\
        .load(source_url)

    df = df.withColumnRenamed('distance_travelled(km)', 'distance_travelled_km')

    df = df.withColumn('bronze_ingested_time', current_timestamp())\
        .withColumn('filename', col('_metadata.file_path'))

    return df