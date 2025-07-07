from ingest import load_data

def clean_data(df):
    df = df.dropna()
    df = df.withColumn("expected_amount", (df["quantity"] * df["price"]) - df["discount"])
    return df

if __name__ == "__main__":
    df = load_data()
    df = clean_data(df)
    df.show()
