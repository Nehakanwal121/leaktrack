import io
import altair as alt
import streamlit as st
import pandas as pd
from src.transform import transform_df  # Make sure this function accepts a Pandas DataFrame

st.set_page_config(page_title="LeakTrack – Revenue Mismatch Analyzer")

st.title("📊 LeakTrack – Revenue Mismatch Detector")

# --- FILE UPLOAD ---
uploaded_file = st.file_uploader("Upload Invoice CSV", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

    # --- TRANSFORMATION LOGIC ---
    df_result = transform_df(df)  # Function from src/transform.py must handle Pandas input

    # --- DOWNLOAD BUTTON ---
    csv = df_result.to_csv(index=False)
    st.download_button(
        label="📥 Download Mismatches as CSV",
        data=csv.encode('utf-8'),
        file_name='mismatches.csv',
        mime='text/csv',
    )

    # --- METRICS DISPLAY ---
    total_mismatches = df_result[df_result['mismatch_flag']].shape[0]
    total_loss = (df_result['expected_amount'] - df_result['billing_amount']).sum()

    st.markdown("### 📊 Summary")
    col1, col2 = st.columns(2)
    col1.metric("Total Mismatches", total_mismatches)
    col2.metric("Estimated Revenue Leakage", f"₹{total_loss:,.2f}")

    # --- BAR CHART ---
    st.markdown("### 📈 Mismatch Overview Chart")
    chart = alt.Chart(df_result).mark_bar().encode(
        x=alt.X('invoice_id:N', title='Invoice ID'),
        y=alt.Y('expected_amount:Q', title='Expected Amount'),
        color=alt.Color('mismatch_flag:N', title='Mismatch'),
        tooltip=['invoice_id', 'expected_amount', 'billing_amount', 'mismatch_flag']
    ).properties(width=700)

    st.altair_chart(chart, use_container_width=True)

else:
    st.info("👆 Please upload a CSV file to begin analysis.")
