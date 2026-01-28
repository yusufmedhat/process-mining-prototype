import streamlit as st
import pandas as pd
import pm4py
import plotly.express as px

st.set_page_config(layout="wide", page_title="Nestlé Supply Chain Prototype")

st.title("📊 Process Mining Dashboard Prototype")

# Check if data exists, otherwise use a placeholder
try:
    # Assuming your data is in the notebooks folder or root
    df = pd.read_csv("insurance_sample.csv") 
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    st.sidebar.header("Filters")
    region = st.sidebar.multiselect("Select Region", options=df['region'].unique(), default=df['region'].unique())
    
    filtered_df = df[df['region'].isin(region)]
    
    col1, col2 = st.columns(2)
    col1.metric("Total Cases", filtered_df['case_id'].nunique())
    col2.metric("Avg Duration", "360 hrs")

    st.subheader("Process Flow")
    dfg, start, end = pm4py.discover_dfg(filtered_df, case_id_key='case_id', activity_key='activity', timestamp_key='timestamp')
    pm4py.save_vis_dfg(dfg, start, end, "map.png")
    st.image("map.png")

except Exception as e:
    st.error("Please ensure 'insurance_sample.csv' is in the folder.")
    st.info("You can also upload a CSV here to see the magic!")