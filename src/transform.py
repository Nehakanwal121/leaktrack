from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark session
spark = SparkSession.builder.appName("LeakTrack-Transform").getOrCreate()

# Load data from CSV again
df = spark.read.csv("data/invoices.csv", header=True, inferSchema=True)

# Add expected billing column: (quantity * price) - discount
df_transformed = df.withColumn("expected_amount", (col("quantity") * col("price")) - col("discount"))

# Flag mismatches
df_result = df_transformed.withColumn("mismatch_flag", expr("expected_amount != billing_amount"))

# Show mismatches only
df_result.filter("mismatch_flag = true").show()

# Save to CSV (for dashboard use)
df_result.write.mode("overwrite").option("header", True).csv("output/mismatches")
