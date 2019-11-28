import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pyspark.sql.functions import *

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

    print('run spark job')
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "new_topic") \
        .option("startingOffsets", "earliest")\
        .option("maxOffsetsPerTrigger", 200)\
        .load()
      
    print('df created')

    df.printSchema()
    
    kafka_df = df.selectExpr("CAST(value AS string)")

    print('kafka_df created')
    kafka_df.printSchema()

    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    print('service table created')
    service_table.printSchema()
    
    #query1st = service_table.writeStream.format('console').start()
    #query1st.awaitTermination()
    
    distinct_table = service_table.select(['call_date_time','original_crime_type_name', 'disposition'])  # need update

    #query2nd = distinct_table.writeStream.format('console').start()
    #query2nd.awaitTermination()

    # count the number of original crime type
    agg_df = distinct_table.select(distinct_table.call_date_time.cast('timestamp'),\
                                    'original_crime_type_name', 'disposition')\
                            .withWatermark('call_date_time', '10 minutes')\
                            .groupBy(
                                window('call_date_time', '10 minutes'),
                                'original_crime_type_name')\
                            .count()
    agg_df.printSchema()
    

    query3rd = agg_df.writeStream.format('console').start()
    query3rd.awaitTermination()

    # joining table 

    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)

    print("radio code schema:")
    radio_code_df.printSchema()

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    print('radio code schema after rename:')
    radio_code_df.printSchema()

    join_query = distinct_table.join(radio_code_df, 'disposition')

    print('join query schema:')
    join_query.printSchema()

    join_query.writeStream.format('console').start().awaitTermination()

    

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
