from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def transform():
    spark = SparkSession.builder.appName("LeakTrack-Transform").getOrCreate()

    # Load data
    df = spark.read.csv("data/invoices.csv", header=True, inferSchema=True)

    # Transform
    df_transformed = df.withColumn("expected_amount", (col("quantity") * col("price")) - col("discount"))
    df_result = df_transformed.withColumn("mismatch_flag", expr("expected_amount != billing_amount"))

    # Save output
    df_result.write.mode("overwrite").option("header", True).csv("output/mismatches")

    return df_result.toPandas()  # send to Streamlit as Pandas DataFrame
