from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

def transform():
    spark = SparkSession.builder.appName("LeakTrack-Transform").getOrCreate()
    df = spark.read.csv("data/invoices.csv", header=True, inferSchema=True)

    # Add expected amount and mismatch flag
    df = df.withColumn("expected_amount", (col("quantity") * col("price")) - col("discount"))
    df = df.withColumn("mismatch_flag", expr("expected_amount != billing_amount"))

    # Reason tagging
    df = df.withColumn("reason", when(col("expected_amount") > col("billing_amount"), "Underbilling")
                                 .when(col("expected_amount") < col("billing_amount"), "Overbilling")
                                 .otherwise("No mismatch"))

    # Risk level tagging
    df = df.withColumn("risk_level", when((col("expected_amount") - col("billing_amount")) > 1000, "🔴 High")
                                     .when((col("expected_amount") - col("billing_amount")) > 500, "🟠 Medium")
                                     .when((col("expected_amount") - col("billing_amount")) > 0, "🟡 Low")
                                     .otherwise("✅ No Risk"))

    # Optional: Alert for large losses (comment out if not needed)
    # from src.alert import send_email_alert
    # critical = df.filter((col("expected_amount") - col("billing_amount")) > 1000)
    # if critical.count() > 0:
    #     send_email_alert("🚨 High Mismatch Detected", f"{critical.count()} high mismatches found.")

    df.write.mode("overwrite").option("header", True).csv("output/mismatches")
    return df.toPandas()
