import streamlit as st
import altair as alt
from src.transform import transform

# Set Streamlit app title
st.set_page_config(page_title="LeakTrack - Revenue Mismatch Detector", layout="centered")
st.title("🔍 LeakTrack - Revenue Mismatch Analyzer")

# Run transformation and load data
df_result = transform()

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
col2.metric("Estimated Revenue Leakage", f"₹{total_loss}")

# --- BAR CHART ---
st.markdown("### 📈 Mismatch Overview Chart")
chart = alt.Chart(df_result).mark_bar().encode(
    x='invoice_id:N',
    y='expected_amount:Q',
    color='mismatch_flag:N',
    tooltip=['invoice_id', 'expected_amount', 'billing_amount', 'mismatch_flag']
).properties(width=700)

st.altair_chart(chart, use_container_width=True)
