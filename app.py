import streamlit as st
import pandas as pd
import pm4py
import plotly.express as px
import os

# 1. Page Configuration
st.set_page_config(layout="wide", page_title="Nestlé Supply Chain Prototype")

st.title("📊 Process Mining Dashboard Prototype")
st.markdown("""
This dashboard visualizes the **Insurance Claims Event Log**, identifying bottlenecks 
and process variants to optimize operational efficiency.
""")

# 2. Data Loading Logic
# Update this path if you move your file!
DATA_PATH = "data/Insurance_claims_event_log.csv"

if os.path.exists(DATA_PATH):
    # Load data
    df = pd.read_csv(DATA_PATH)
    
    # Ensure timestamp is in datetime format
    # Replace 'timestamp' with the actual column name in your CSV if different
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 3. Sidebar Filters
    st.sidebar.header("Filter Options")
    
    # Replace 'region' with the actual column name in your CSV if different
    if 'region' in df.columns:
        regions = st.sidebar.multiselect(
            "Select Region", 
            options=df['region'].unique(), 
            default=df['region'].unique()
        )
        filtered_df = df[df['region'].isin(regions)]
    else:
        filtered_df = df

    # 4. Key Metrics
    col1, col2, col3 = st.columns(3)
    # Replace 'case_id' with your actual Case ID column name
    col1.metric("Total Cases", filtered_df['case_id'].nunique())
    col2.metric("Total Events", len(filtered_df))
    col3.metric("Avg Throughput", "Calculated")

    # 5. Process Graph (The "Magic" part)
    st.subheader("🕸️ Process Flow Discovery")
    
    # We use DFG (Directly Follows Graph) for the visualization
    dfg, start, end = pm4py.discover_dfg(
        filtered_df, 
        case_id_key='case_id', 
        activity_key='activity', 
        timestamp_key='timestamp'
    )
    
    # Save the visualization as a temporary image
    pm4py.save_vis_dfg(dfg, start, end, "process_map.png")
    
    # Display the image in the dashboard
    st.image("process_map.png", use_container_width=True)

    # 6. Activity Distribution
    st.subheader("📈 Activity Frequency")
    activity_counts = filtered_df['activity'].value_counts().reset_index()
    fig = px.bar(activity_counts, x='activity', y='count', color='activity', title="Events per Activity")
    st.plotly_chart(fig, use_container_width=True)

else:
    st.error(f"❌ File not found: `{DATA_PATH}`")
    st.warning("Please ensure your CSV is uploaded to the 'data' folder on GitHub.")
    st.info(f"Current Directory Contents: {os.listdir('.')}")
    if os.path.exists("data"):
        st.info(f"Contents of /data folder: {os.listdir('data')}")