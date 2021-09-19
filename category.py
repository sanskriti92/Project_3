from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

print("Stream Data Processing Application Started ...")

spark = SparkSession \
    .builder \
    .appName("Kafka Project3") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "project3") \
    .option("startingOffsets", "latest") \
    .load()

print("Printing Schema of orders_df: ")
df.printSchema()

df1 = df.selectExpr("CAST(value AS STRING)")
print("Printing Schema of orders_df1: ")
df1.printSchema()

orders_schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("customer_id", IntegerType())\
    .add("customer_name", StringType()) \
    .add("product_id", IntegerType())\
    .add("product_name", StringType()) \
    .add("product_category", StringType()) \
    .add("payment_type", StringType()) \
    .add("qty", IntegerType()) \
    .add("price", DoubleType()) \
    .add("datetime", TimestampType())\
    .add("country", StringType())\
    .add("city", StringType())\
    .add("ecommerce_website_name", StringType())\
    .add("payment_txn_id", StringType())\
    .add("payment_txn_success", StringType())\
    .add("failure_reason", StringType())


df2 = df1.select(from_json(col("value"), orders_schema).alias("orders"))
print("Printing Schema of orders_df2: ")
df2.printSchema()

df3 = df2.select("orders.*")
print("Printing Schema of orders_df3: ")
df3.printSchema()

topics = ["Card", "InternetBanking","Wallet","UPI","Gpay","PhonePe","COD","Y","N"]

for pay_type in topics:
    check_location="/home/hdoop/tmp/"+pay_type

    if pay_type == "Y" or pay_type == "N":
        col_value="payment_txn_success"

        if pay_type=="Y":
            topic_name="success"
        else:
            topic_name="failure"
    else:
        col_value="payment_type"
        topic_name=pay_type
    
    df4 = df3.filter(col(col_value) == pay_type)
    result = df4.select(to_json(struct("*")).alias("value"))\
        .writeStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", topic_name)\
        .option("checkpointLocation", check_location)\
        .trigger(processingTime='5 seconds')\
        .start()

result.awaitTermination()
print("Stream Data Processing Application Completed.")


