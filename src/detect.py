from clean import clean_data
from ingest import load_data
from pyspark.sql.functions import col

def detect_leakage(df):
    df = df.withColumn("leak", col("expected_amount") != col("billing_amount"))
    return df.filter(col("leak") == True)

if __name__ == "__main__":
    df = clean_data(load_data())
    leaked = detect_leakage(df)
    leaked.show()
