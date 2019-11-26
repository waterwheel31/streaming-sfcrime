import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    print('run spark job')
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "new_topic") \
        .load()
       # .option("maxOffsetsPerTrigger", 200)
      

    print('df created')

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS string)")

    print('kafka_df created')

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    print('service table created')
    service_table.printSchema()
    
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(['original_crime_type_name', 'disposition'])  # need update

    # count the number of original crime type
    #agg_df = distinct_table.groupBy('original_crime_type_name').count()
    agg_df = distinct_table 
     
        #.withWatermark('call_date_time', '20 seconds')\
        
    print('agg_df created')

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream\
        .trigger(once=True)\
        .format('console')\
        .start()

    print('query created')



    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./data_stream.py"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    print('radio code:', radio_code_df)

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, 'desposition')
    join_query.writeStream.start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    print('main started')

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    print('sparkSession created\n')

    logger.info("Spark started")

    run_spark_job(spark)

    print('spark job run!')

    spark.stop()
