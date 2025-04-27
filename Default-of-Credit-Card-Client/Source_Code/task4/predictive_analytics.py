from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Credit Default Prediction") \
    .getOrCreate()

# Step 2: Load the dataset
file_path = "credit_card.csv"
df = spark.read.option("header", "true").csv(file_path)

# Cast columns to appropriate types
for column in df.columns:
    df = df.withColumn(column, col(column).cast("integer"))

# Step 3: Data Preprocessing
# Check for missing values (not expected, but included for robustness)
for column in df.columns:
    missing_count = df.filter(col(column).isNull()).count()
    print(f"{column} has {missing_count} missing values")

# Rename target column for clarity
df = df.withColumnRenamed("default payment next month", "label")

# Step 4: Feature Engineering
# Select features for prediction
feature_cols = ["LIMIT_BAL", "SEX", "EDUCATION", "MARRIAGE", "AGE",
                "PAY_0", "PAY_2", "PAY_3", "PAY_4", "PAY_5", "PAY_6",
                "BILL_AMT1", "BILL_AMT2", "BILL_AMT3", "BILL_AMT4", "BILL_AMT5", "BILL_AMT6",
                "PAY_AMT1", "PAY_AMT2", "PAY_AMT3", "PAY_AMT4", "PAY_AMT5", "PAY_AMT6"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)

# Select only necessary columns
df = df.select("features", "label")

# Step 5: Split data into training and test sets
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Step 6: Build and train the model
lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
model = lr.fit(train_df)

# Step 7: Make predictions
predictions = model.transform(test_df)

# Step 8: Evaluate the model
# Binary classification metrics (AUC-ROC)
binary_evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc_roc = binary_evaluator.evaluate(predictions)
print(f"Area Under ROC: {auc_roc:.4f}")

# Multiclass classification metrics (Accuracy, Precision, Recall, F1)
multi_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})

print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"F1-Score: {f1:.4f}")

# Show sample predictions
print("Sample Predictions:")
predictions.select("features", "label", "prediction", "probability").show(5, truncate=False)

# Step 9: Stop Spark session
spark.stop()
