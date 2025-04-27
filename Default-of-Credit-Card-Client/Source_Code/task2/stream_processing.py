from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when
from pyspark.sql.types import StructType, StructField, IntegerType

# Initialize Spark Session with Streaming support
spark = SparkSession.builder \
    .appName("Real-Time Credit Card Analytics") \
    .getOrCreate()

# Define schema based on credit_card.csv
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("LIMIT_BAL", IntegerType(), True),
    StructField("SEX", IntegerType(), True),
    StructField("EDUCATION", IntegerType(), True),
    StructField("MARRIAGE", IntegerType(), True),
    StructField("AGE", IntegerType(), True),
    StructField("PAY_0", IntegerType(), True),
    StructField("PAY_2", IntegerType(), True),
    StructField("PAY_3", IntegerType(), True),
    StructField("PAY_4", IntegerType(), True),
    StructField("PAY_5", IntegerType(), True),
    StructField("PAY_6", IntegerType(), True),
    StructField("BILL_AMT1", IntegerType(), True),
    StructField("BILL_AMT2", IntegerType(), True),
    StructField("BILL_AMT3", IntegerType(), True),
    StructField("BILL_AMT4", IntegerType(), True),
    StructField("BILL_AMT5", IntegerType(), True),
    StructField("BILL_AMT6", IntegerType(), True),
    StructField("PAY_AMT1", IntegerType(), True),
    StructField("PAY_AMT2", IntegerType(), True),
    StructField("PAY_AMT3", IntegerType(), True),
    StructField("PAY_AMT4", IntegerType(), True),
    StructField("PAY_AMT5", IntegerType(), True),
    StructField("PAY_AMT6", IntegerType(), True),
    StructField("default payment next month", IntegerType(), True)
])

# Read stream from directory
stream_df = spark.readStream \
    .schema(schema) \
    .option("header", "true") \
    .csv("stream_input")

# Process the stream
def process_batch(batch_df, batch_id):
    print(f"\nProcessing Batch {batch_id}")

    # Trend: Running average of bill amounts
    avg_bill = batch_df.select(mean("BILL_AMT1").alias("avg_bill_amt1")).collect()[0]["avg_bill_amt1"]
    print(f"Average BILL_AMT1 in this batch: {avg_bill:.2f}")

    # Anomaly: Detect unusually high payments (e.g., > 50,000)
    anomalies = batch_df.filter(col("PAY_AMT1") > 50000) \
        .select("ID", "PAY_AMT1", "LIMIT_BAL")
    if anomalies.count() > 0:
        print("Anomalies (PAY_AMT1 > 50,000):")
        anomalies.show()

    # Summary Statistics: Count defaults and total records
    default_count = batch_df.filter(col("default payment next month") == 1).count()
    total_count = batch_df.count()
    print(f"Summary Statistics - Defaults: {default_count}, Total Records: {total_count}")

# Apply processing to each micro-batch
query = stream_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# Keep the stream running
query.awaitTermination()
