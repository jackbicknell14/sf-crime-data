import logging
import json
from pyspark.sql import SparkSession
import pyspark.sql.types as pst
import pyspark.sql.functions as psf


schema = pst.StructType([
    pst.StructField("crime_id", pst.StringType(), True),
    pst.StructField("original_crime_type_name", pst.StringType(), True),
    pst.StructField("report_date", pst.DateType(), True),
    pst.StructField("call_date", pst.DateType(), True),  
    pst.StructField("offense_date", pst.DateType(), True),
    pst.StructField("call_time", pst.StringType(), True), 
    pst.StructField("call_date_time", pst.TimestampType(), True),  
    pst.StructField("disposition", pst.StringType(), True),  
    pst.StructField("address", pst.StringType(), True),  
    pst.StructField("city", pst.StringType(), True),  
    pst.StructField("state", pst.StringType(), True), 
    pst.StructField("agency_id", pst.StringType(), True),  
    pst.StructField("address_type", pst.StringType(), True),
    pst.StructField("common_location", pst.StringType(), True),  
])

def run_spark_job(spark):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 1000)\
        .option("spark.sql.inMemoryColumnarStorage.batchSize", 100000)\
        .option("spark.sql.shuffle.partitions", 1000)\
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    
    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
                psf.col('original_crime_type_name'), 
                psf.col('disposition'))

    # count the number of original crime type
    agg_df = distinct_table\
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
            psf.col('original_crime_type_name')
        )\
        .count()\
        .sort('count', ascending=False)

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .outputMode('Complete') \
        .format('console') \
        .option("truncate", "false") \
        .trigger(processingTime='10 seconds') \
        .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition)

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .config("spark.sql.shuffle.partitions", 1) \
        .config("spark.default.parallelism", 1) \
        .config("spark.driver.memory","2g") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

