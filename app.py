import streamlit as st
import pandas as pd
import pm4py
import plotly.express as px
import os

st.set_page_config(layout="wide", page_title="Nestlé Supply Chain Prototype")

st.title("📊 Process Mining Dashboard Prototype")

DATA_PATH = "data/Insurance_claims_event_log.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    
    # --- EXACT COLUMN MAPPING FOR YOUR DATA ---
    # We map your specific names to the ones pm4py expects
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # We'll use these variables to keep the discovery code clean
    case_col = 'case_id'
    act_col = 'activity_name'  # Updated from 'activity' to match your CSV
    time_col = 'timestamp'

    # --- SIDEBAR FILTERS ---
    st.sidebar.header("Filter Options")
    
    # Using 'type_of_policy' or 'type_of_accident' as a filter instead of region
    filter_col = 'type_of_policy' 
    if filter_col in df.columns:
        options = st.sidebar.multiselect(
            f"Select {filter_col}", 
            options=df[filter_col].unique(), 
            default=df[filter_col].unique()
        )
        filtered_df = df[df[filter_col].isin(options)]
    else:
        filtered_df = df

    # --- KEY METRICS ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Cases", filtered_df[case_col].nunique())
    col2.metric("Total Events", len(filtered_df))
    col3.metric("Avg Claim Amount", f"${filtered_df['claim_amount'].mean():,.2f}")

    # --- PROCESS GRAPH ---
    st.subheader("🕸️ Process Flow Discovery")
    
    # Discovering the Directly Follows Graph (DFG)
    dfg, start, end = pm4py.discover_dfg(
        filtered_df, 
        case_id_key=case_col, 
        activity_key=act_col, 
        timestamp_key=time_col
    )
    
    # Visualization
    pm4py.save_vis_dfg(dfg, start, end, "process_map.png")
    st.image("process_map.png", use_container_width=True)

    # --- ADDITIONAL INSIGHT ---
    st.subheader("🚗 Top Car Makes in Claims")
    car_counts = filtered_df['car_make'].value_counts().reset_index().head(10)
    fig = px.bar(car_counts, x='car_make', y='count', color='car_make')
    st.plotly_chart(fig, use_container_width=True)

else:
    st.error(f"❌ File not found: `{DATA_PATH}`")
