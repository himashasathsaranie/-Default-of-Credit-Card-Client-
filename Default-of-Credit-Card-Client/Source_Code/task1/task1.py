from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from pyspark.sql.types import IntegerType, DoubleType
import pandas as pd
import time

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Credit Card Default Preprocessing") \
    .getOrCreate()

# Step 2: Load the CSV file into a Spark DataFrame
file_path = "credit_card.csv"  # Adjust path if needed
df = spark.read.option("header", "true").csv(file_path)

# Cast columns to appropriate data types (all are integers in this dataset)
for column in df.columns:
    df = df.withColumn(column, col(column).cast(IntegerType()))

# Step 3: Data Exploration
print("Initial Data Schema:")
df.printSchema()

print("First 5 rows:")
df.show(5)

# Step 4: Data Cleansing
# Check for missing values
print("Checking for missing values:")
for column in df.columns:
    missing_count = df.filter(col(column).isNull()).count()
    if missing_count > 0:
        print(f"{column} has {missing_count} missing values")
    else:
        print(f"{column} has no missing values")

# No missing values in this dataset as per description, but let's handle hypothetically
# Example: Replace nulls with mean (if any existed)
for column in df.columns:
    mean_val = df.select(mean(col(column))).collect()[0][0]
    df = df.na.fill({column: mean_val})

# Remove duplicates (if any)
df = df.dropDuplicates()

# Step 5: Data Transformation
# Rename columns for clarity
df = df.withColumnRenamed("default payment next month", "default_next_month") \
       .withColumnRenamed("LIMIT_BAL", "credit_limit") \
       .withColumnRenamed("PAY_0", "pay_status_sept") \
       .withColumnRenamed("PAY_2", "pay_status_aug") \
       .withColumnRenamed("PAY_3", "pay_status_july")

# Categorize credit limit into bins
df = df.withColumn("credit_limit_category",
                   when(col("credit_limit") <= 50000, "Low")
                   .when((col("credit_limit") > 50000) & (col("credit_limit") <= 200000), "Medium")
                   .otherwise("High"))

# Step 6: Feature Engineering
# Create a feature: Total bill amount across all months
df = df.withColumn("total_bill_amount",
                   col("BILL_AMT1") + col("BILL_AMT2") + col("BILL_AMT3") +
                   col("BILL_AMT4") + col("BILL_AMT5") + col("BILL_AMT6"))

# Create a feature: Average payment status
df = df.withColumn("avg_payment_status",
                   (col("pay_status_sept") + col("pay_status_aug") + col("pay_status_july") +
                    col("PAY_4") + col("PAY_5") + col("PAY_6")) / 6.0)

# Step 7: Final Preprocessed Data
print("Preprocessed Data Schema:")
df.printSchema()

print("First 5 rows of preprocessed data:")
df.show(5)

# Step 8: Performance Comparison with Pandas
# Load data with Pandas
start_time_pandas = time.time()
pandas_df = pd.read_csv(file_path)
# Basic preprocessing in Pandas (same steps as above)
pandas_df = pandas_df.dropna()  # Drop missing values (none in this case)
pandas_df = pandas_df.drop_duplicates()
pandas_df["credit_limit_category"] = pd.cut(pandas_df["LIMIT_BAL"], 
                                            bins=[0, 50000, 200000, float("inf")], 
                                            labels=["Low", "Medium", "High"])
pandas_df["total_bill_amount"] = (pandas_df["BILL_AMT1"] + pandas_df["BILL_AMT2"] + 
                                  pandas_df["BILL_AMT3"] + pandas_df["BILL_AMT4"] + 
                                  pandas_df["BILL_AMT5"] + pandas_df["BILL_AMT6"])
end_time_pandas = time.time()

pandas_time = end_time_pandas - start_time_pandas
print(f"Pandas processing time: {pandas_time:.2f} seconds")

# Spark processing time (already included in execution)
start_time_spark = time.time()
df.count()  # Force computation to measure time
end_time_spark = time.time()
spark_time = end_time_spark - start_time_spark
print(f"Spark processing time: {spark_time:.2f} seconds")

# Step 9: Save the preprocessed Spark DataFrame (optional)
df.write.mode("overwrite").parquet("credit_card_preprocessed.parquet")

# Stop the Spark session
spark.stop()
