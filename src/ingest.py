from pyspark.sql import SparkSession

def load_data():
    spark = SparkSession.builder.appName("LeakTrack").getOrCreate()
    df = spark.read.csv("data/raw/retail_invoices.csv", header=True, inferSchema=True)
    return df

if __name__ == "__main__":
    df = load_data()
    df.show()

df.write.mode("overwrite").option("header", True).csv("data")
