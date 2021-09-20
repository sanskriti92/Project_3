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

#  root
#  |-- key: binary (nullable = true)
#  |-- value: binary (nullable = true)
#  |-- topic: string (nullable = true)
#  |-- partition: integer (nullable = true)
#  |-- offset: long (nullable = true)
#  |-- timestamp: timestamp (nullable = true)
#  |-- timestampType: integer (nullable = true)


df1 = df.selectExpr("CAST(value AS STRING)" )
print("Printing Schema of orders_df1: ")
df1.printSchema()

# root
#  |-- value: string (nullable = true)

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


df2 = df1\
    .select(from_json(col("value"), orders_schema)\
    .alias("orders") )
print("Printing Schema of orders_df2: ")
df2.printSchema()
# Nested columns
# root
#  |-- orders: struct (nullable = true)
#  |    |-- order_id: integer (nullable = true)
#  |    |-- customer_id: integer (nullable = true)
#  |    |-- customer_name: string (nullable = true)
#  |    |-- product_id: integer (nullable = true)
#  |    |-- product_name: string (nullable = true)
#  |    |-- product_category: string (nullable = true)
#  |    |-- payment_type: string (nullable = true)
#  |    |-- qty: integer (nullable = true)
#  |    |-- price: double (nullable = true)
#  |    |-- datetime: timestamp (nullable = true)
#  |    |-- country: string (nullable = true)
#  |    |-- city: string (nullable = true)
#  |    |-- ecommerce_website_name: string (nullable = true)
#  |    |-- payment_txn_id: string (nullable = true)
#  |    |-- payment_txn_success: string (nullable = true)
#  |    |-- failure_reason: string (nullable = true)



df3 = df2.select("orders.*")
print("Printing Schema of orders_df3: ")
df3.printSchema()
# root
#  |-- order_id: integer (nullable = true)
#  |-- customer_id: integer (nullable = true)
#  |-- customer_name: string (nullable = true)
#  |-- product_id: integer (nullable = true)
#  |-- product_name: string (nullable = true)
#  |-- product_category: string (nullable = true)
#  |-- payment_type: string (nullable = true)
#  |-- qty: integer (nullable = true)
#  |-- price: double (nullable = true)
#  |-- datetime: timestamp (nullable = true)
#  |-- country: string (nullable = true)
#  |-- city: string (nullable = true)
#  |-- ecommerce_website_name: string (nullable = true)
#  |-- payment_txn_id: string (nullable = true)
#  |-- payment_txn_success: string (nullable = true)
#  |-- failure_reason: string (nullable = true)


df4 = df3.groupBy( col("city"), col("payment_type") ) \
    .agg(sum("price").alias("Total_price") ,\
    count("order_id").alias("Total_orders") ) \
    .select("city", "Total_orders", "payment_type", "Total_price")

print("Printing Schema of orders_df4: ")
df4.printSchema()

# root
#  |-- city: string (nullable = true)
#  |-- Total_orders: long (nullable = false)
#  |-- payment_type: string (nullable = true)
#  |-- Total_price: double (nullable = true)


#// Filesink only support Append mode.
result = df3\
  .writeStream\
  .format("csv")\
  .trigger(processingTime="40 seconds")\
  .option("checkpointLocation", "output/checkpoint")\
  .option("path", "output/filesink_output")\
  .outputMode("append")\
  .start()


# 3 output_modes --> append, update, complete
cons = df4.orderBy("city","payment_type") \
    .writeStream \
    .trigger(processingTime='20 seconds')\
    .outputMode("complete")\
    .option("truncate", "false")\
    .option("numRows", 100)\
    .format("console") \
    .start()


df5 = df3.groupBy( "ecommerce_website_name" ) \
    .agg(count("order_id").alias("Number_of_orders_placed") )\
    .select("ecommerce_website_name","Number_of_orders_placed" )

cons = df5.orderBy(col("Number_of_orders_placed").desc()) \
    .writeStream \
    .trigger(processingTime='20 seconds')\
    .outputMode("complete")\
    .option("truncate", "false")\
    .option("numRows", 100)\
    .format("console") \
    .start()

cons.awaitTermination()
print("Stream Data Processing Application Completed.")
