import streamlit as st
import altair as alt
import pandas as pd
from src.transform import transform  # Your transformation logic

# 1️⃣ --- PAGE SETUP ---
st.set_page_config(page_title="LeakTrack - Revenue Mismatch Detector", layout="centered")
st.title("🔍 LeakTrack - Revenue Mismatch Analyzer")

# 2️⃣ --- LOAD & TRANSFORM DATA ---
df_result = transform()
# --- SMART INSIGHTS ---
st.markdown("### 🧠 Smart Insights")

underbilling_count = df_result[df_result['reason'] == 'Underbilling'].shape[0]
overbilling_count = df_result[df_result['reason'] == 'Overbilling'].shape[0]
high_risk_count = df_result[df_result['risk_level'] == '🔴 High'].shape[0]
top_invoice = df_result[df_result['expected_amount'] - df_result['billing_amount'] == 
                        (df_result['expected_amount'] - df_result['billing_amount']).max()]

if not top_invoice.empty:
    top_id = top_invoice['invoice_id'].values[0]
    top_loss = top_invoice['expected_amount'].values[0] - top_invoice['billing_amount'].values[0]
    st.info(f"💡 Largest mismatch is Invoice #{top_id} with a revenue leakage of ₹{top_loss:.2f}")

st.markdown(f"""
- 🔻 **Underbilling cases:** {underbilling_count}
- 🔺 **Overbilling cases:** {overbilling_count}
- 🔴 **High-risk mismatches (>₹1000):** {high_risk_count}
""")


# 3️⃣ --- CSV DOWNLOAD BUTTON ---
csv = df_result.to_csv(index=False)
st.download_button(
    label="📥 Download Mismatches as CSV",
    data=csv.encode('utf-8'),
    file_name='mismatches.csv',
    mime='text/csv',
)


# 4️⃣ --- METRICS DISPLAY ---
total_mismatches = df_result[df_result['mismatch_flag']].shape[0]
total_loss = (df_result['expected_amount'] - df_result['billing_amount']).sum()

st.markdown("### 📊 Summary")
col1, col2 = st.columns(2)
col1.metric("Total Mismatches", total_mismatches)
col2.metric("Estimated Revenue Leakage", f"₹{total_loss}")

# 5️⃣ --- MISMATCH BAR CHART ---
st.markdown("### 📈 Mismatch Overview Chart")
chart = alt.Chart(df_result).mark_bar().encode(
    x='invoice_id:N',
    y='expected_amount:Q',
    color='mismatch_flag:N',
    tooltip=['invoice_id', 'expected_amount', 'billing_amount', 'mismatch_flag']
).properties(width=700)
st.altair_chart(chart, use_container_width=True)

# 6️⃣ --- MISMATCH TREND CHART (STEP 3) ---
st.markdown("### 📉 Mismatch Trend Over Time")
if 'invoice_date' in df_result.columns:
    df_result['invoice_date'] = pd.to_datetime(df_result['invoice_date'], errors='coerce')
    trend = df_result[df_result['mismatch_flag']]
    trend = trend.groupby(trend['invoice_date'].dt.to_period("M")).size().reset_index(name="count")
    trend['invoice_date'] = trend['invoice_date'].astype(str)

    trend_chart = alt.Chart(trend).mark_line(point=True).encode(
        x='invoice_date:T',
        y='count:Q'
    ).properties(width=700)
    st.altair_chart(trend_chart, use_container_width=True)
else:
    st.info("📅 No `invoice_date` column found for trend analysis.")

# 7️⃣ --- FILTERING (STEP 4) ---
st.sidebar.header("🔎 Filter Options")
mismatch_filter = st.sidebar.selectbox("Mismatch Type", ["All", "Underbilling", "Overbilling"])
min_amt = st.sidebar.number_input("Minimum Expected Amount", value=0)

filtered_df = df_result.copy()
if mismatch_filter != "All":
    filtered_df = filtered_df[filtered_df['reason'] == mismatch_filter]
filtered_df = filtered_df[filtered_df['expected_amount'] >= min_amt]
st.markdown("### 🗃️ Filtered Mismatches with Risk Level")
st.dataframe(filtered_df[['invoice_id', 'expected_amount', 'billing_amount', 'reason', 'risk_level']])


# 8️⃣ --- PDF REPORT GENERATION (STEP 5) ---
from fpdf import FPDF

def generate_pdf_report(df, filename="report.pdf"):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="LeakTrack - Revenue Mismatch Report", ln=True, align='C')

    for index, row in df.iterrows():
        pdf.cell(200, 10, txt=f"Invoice {row['invoice_id']}: {row['reason']} (₹{row['expected_amount']} vs ₹{row['billing_amount']})", ln=True)

    pdf.output(filename)

if st.button("📄 Generate PDF Report"):
    generate_pdf_report(df_result[df_result['mismatch_flag']])
    st.success("✅ PDF saved as `report.pdf` in the project directory.")
st.markdown("### 🧯 Risk Level Distribution")
risk_chart = alt.Chart(df_result).mark_arc().encode(
    theta="count():Q",
    color="risk_level:N"
).properties(width=400)
st.altair_chart(risk_chart)
