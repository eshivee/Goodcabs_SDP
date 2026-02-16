from pyspark.sql.functions import *
from pyspark import pipelines as dp

start_date = spark.conf.get('start_date')
end_date = spark.conf.get('end_date')

@dp.materialized_view(
    name = 'goodcabs.silver.date_trip',
    comment= 'silver layer for date_trip',
    table_properties= {
        'layer' : 'silver',
        'quality' : 'siver',
        'delta.enableChangeDataFeed' : 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact' : 'true'
    }
)

def date_trip_silver():
    df = spark.sql(f"""
                   SELECT EXPLODE(SEQUENCE(to_date('{start_date}'), to_date('{end_date}'), INTERVAL 1 DAY)) AS date
                   """)
    
    df = df.withColumn(
        'datekey', date_format(col('date'), 'yyyyMMdd').cast('int')
    )

    df = df.withColumn('year', year('date'))\
            .withColumn('month', month('date'))\
            .withColumn('quarter', quarter('date'))
    
    df = df.withColumn('day_of_month', dayofmonth('date'))\
            .withColumn('day_of_week', date_format(col('date'), 'EEEE'))\
            .withColumn('day_of_week_abbr', date_format(col('date'), 'EEE'))\
            .withColumn('day_of_week_num', dayofweek('date'))

    df = df.withColumn('month_name', date_format('date', 'MMMM'))\
            .withColumn('month_year', concat('month_name', lit(' '), year('date')))\
            .withColumn('quarter_year', concat(lit('Q'), quarter('date'), year('date')))

    df = df.withColumn('week_of_the_year', weekofyear('date'))\
            .withColumn('day_of_the_year', dayofyear('date'))

    df = df.withColumn('is_weekend', when(col('day_of_week_num').isin([1,7]), True).otherwise(False))\
            .withColumn('is_weekday', when(col('day_of_week_num').isin([1,7]), False).otherwise(True))

    df = df.withColumn(
        "holiday_name",
        when(
            (col("month") == 6) & (col("day_of_month") == 12), lit("Democracy Day")
        )
        .when(
            (col("month") == 10) & (col("day_of_month") == 1),
            lit("Independence Day"),
        )
        .when(
            (col("month") == 12) & (col("day_of_month") == 25),
            lit("Christmas Day"),
        )
        .when(
            (col("month") == 1) & (col("day_of_month") == 1),
            lit("New Year's Day"))
       .when(
            (col("month") == 5) & (col("day_of_month") == 1),
            lit("Worker's Day")
        )
        .otherwise(None)
    ).withColumn(
        "is_holiday", when(col("holiday_name").isNotNull(), True).otherwise(False)
    )

    df = df.withColumn('silver_processed_date_trip', current_timestamp())

    df_silver = df.select("date",
        "datekey",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "day_of_week_abbr",
        "month_name",
        "month_year",
        "quarter",
        "quarter_year",
        "week_of_the_year",
        "day_of_the_year",
        "is_weekday",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "silver_processed_date_trip")

    return df_silver


