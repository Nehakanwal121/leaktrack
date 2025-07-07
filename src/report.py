from detect import detect_leakage, clean_data, load_data

def save_report(df):
    df.toPandas().to_csv("data/processed/leak_report.csv", index=False)
    print("✅ Leak report saved at: data/processed/leak_report.csv")

if __name__ == "__main__":
    df = clean_data(load_data())
    leak_df = detect_leakage(df)
    save_report(leak_df)
