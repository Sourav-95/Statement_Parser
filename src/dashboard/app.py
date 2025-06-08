import sqlite3
import pandas as pd
import streamlit as st
import altair as alt

# Connect to SQLite
conn = sqlite3.connect("./database/bank_transactions.db")

# Load data of Exp chart
query = "SELECT * FROM TRANSACTION_T WHERE Category != 'Transfer'"
df = pd.read_sql_query(query, conn)

# Close connection
conn.close()

# # Streamlit UI
# st.title("Expense Dashboard")
# chart = alt.Chart(df).mark_bar().encode(
#     x='Category',
#     y='Debit',
#     color='Category'
# )

# st.altair_chart(chart, use_container_width=True)

# # Filter
# category_filter = st.selectbox("Choose a category", df['Category'].unique())

# # Filtered data
# # filtered_df = df[df['Category'] == category_filter]

# # # Show table and chart
# # st.write("Filtered Data", filtered_df)

# # Plot (e.g., count over time)
# st.line_chart(df.set_index('Date')['Debit'])


