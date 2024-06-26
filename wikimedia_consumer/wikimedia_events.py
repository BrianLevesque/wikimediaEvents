from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col


BOOTSTRAP_SERVERS = "confluent-local-broker-1:56443"
TOPIC = "wikimedia_events"


schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("user", StringType()),
    StructField("meta", StructType([
        StructField("domain",StringType())])),
    StructField("length", StructType([
        StructField("old",IntegerType()),
        StructField("new",IntegerType())])
                )])
    
                
    
   


def main():
    spark = SparkSession.builder \
                        .appName('WikimediaEvents') \
                        .getOrCreate()
    kafka_stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .load()
    

    kafka_stream_df = kafka_stream_df.select(F.from_json(F.col("value").cast("string"),schema).alias("data"))\
                           .select("data.timestamp","data.bot", "data.length", "data.minor", "data.user","data.meta")
    
    
    
    df = kafka_stream_df.select(
        col("timestamp"),
        col("bot"),
        col("minor"),
        col("user"),
        col("meta.domain"),
        col("length.new").alias("new_length"),
        col("length.old").alias("old_length")
        )
    df = df.withColumn("length_diff", col("new_length") - col("old_length"))
    df = df.withColumn("length_diff_percent", col("length_diff") / col("old_length") * 100) 

    #Output the top five domains, along with counts for each.

    #domain_df = df.groupBy(col("domain")).count()

    #domain_top5 = domain_df.orderBy(col("count").desc()).limit(5)

    #Output the top five users, based on the length of content they have added (sum of length_diff).

    #user_df = df.groupBy(col("user")).sum("length_diff")

    #user_top5 = user_df.orderBy(col("sum(length_diff)").desc()).limit(5)

    #Output the total number of events, the percent of events by bots, the average length_diff, the minimum length_diff, and the maximum length_diff. (In this example, percent_bot is represented as 0-1, rather than 0-100.
    


    #calculating average, min, max values for 'length_diff" column for bots
    """
    bot_stats = df.select(
        F.count(col("bot")).alias("total_count"),
        F.count_if(col('bot') == True)/F.count(col("bot")).alias("percent_bot"),
        F.avg("length_diff").alias("average_length_diff"),
        F.min("length_diff").alias("min_length_diff"),
        F.max("length_diff").alias("max_length_diff"))
                                              


    
    query = bot_stats \
        .writeStream \
        .outputMode("complete")\
        .format("console") \
        .option("truncate", True) \
        .start() \
        .awaitTermination()
        
"""
    df.writeStream \
        .outputMode("append")\
        .option("checkpointLocation","output")\
        .format("csv")\
        .option("path","./output")\
        .option("header",True)\
        .trigger(processingTime="10 seconds")\
        .start()\
        .awaitTermination()
        
    
if __name__ == "__main__":
    main()