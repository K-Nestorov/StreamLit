import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Sales Dashboard", layout="wide")
st.title("Snowflake Visualization")
st.markdown("Streamlit Dashboard for ETL Data")

session = get_active_session()
query = """
SELECT ORDER_ID, CUSTOMER_ID, CUSTOMER_NAME, ORDER_DATE, PRODUCT, QUANTITY, 
       PRICE, DISCOUNT, TOTAL_AMOUNT, PAYMENT_METHOD, SHIPPING_ADDRESS, STATUS
FROM TD_CLEAN_RECORDS
"""
data = session.sql(query).to_pandas()

data['ORDER_DATE'] = pd.to_datetime(data['ORDER_DATE'], errors='coerce')
data['Month'] = data['ORDER_DATE'].dt.to_period('M').astype(str)

st.sidebar.header("Filters")
date_start, date_end = st.sidebar.date_input("Date Range", [data['ORDER_DATE'].min(), data['ORDER_DATE'].max()])
product_filter = st.sidebar.multiselect("Choose Products", options=data['PRODUCT'].unique(), default=data['PRODUCT'].unique())

filtered = data[
    (data['ORDER_DATE'] >= pd.to_datetime(date_start)) & 
    (data['ORDER_DATE'] <= pd.to_datetime(date_end)) &
    (data['PRODUCT'].isin(product_filter))
]

st.subheader("Key Metrics")
metrics = {
    "Total Revenue": f"${filtered['TOTAL_AMOUNT'].sum():,.2f}",
    "Total Units": f"{filtered['QUANTITY'].sum():,}",
    "Orders": f"{len(filtered):,}",
    "Unique Products": filtered['PRODUCT'].nunique()
}

col1, col2, col3, col4 = st.columns(4)
for idx, (key, value) in enumerate(metrics.items()):
    [col1, col2, col3, col4][idx].metric(key, value)

monthly = filtered.groupby('Month')['TOTAL_AMOUNT'].sum().reset_index()
st.subheader("Monthly Revenue - Bar Chart")
st.bar_chart(monthly, x='Month', y='TOTAL_AMOUNT')

st.subheader("Monthly Revenue - Line Chart")
st.line_chart(monthly.set_index('Month'))

top_products = filtered.groupby('PRODUCT')['QUANTITY'].sum().reset_index().sort_values(by='QUANTITY', ascending=False).head(filtered['PRODUCT'].nunique())
st.subheader("Top Products by Units Sold")
st.bar_chart(top_products, x='PRODUCT', y='QUANTITY')

revenue_share = filtered.groupby('PRODUCT')['TOTAL_AMOUNT'].sum().reset_index().sort_values('TOTAL_AMOUNT', ascending=False)
st.subheader("Revenue Share by Product")
fig, ax = plt.subplots()
ax.pie(revenue_share['TOTAL_AMOUNT'], labels=revenue_share['PRODUCT'], autopct='%1.1f%%', startangle=10)
ax.axis('equal') 
st.pyplot(fig)

csv = filtered.to_csv(index=False).encode('utf-8')
st.download_button("Download Filtered Data as CSV", csv, "filtered_data.csv", "text/csv")

with st.expander("Show Raw Data"):
    st.dataframe(filtered, use_container_width=True)
