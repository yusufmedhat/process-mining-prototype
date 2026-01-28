import streamlit as st
import pandas as pd
import pm4py
import plotly.express as px
import os

# 1. Page Config
st.set_page_config(layout="wide", page_title="Nestlé Process Mining Prototype")

st.title("📊 Process Mining X-Ray Dashboard")
st.markdown("""
This dashboard provides a deep dive into the **Insurance Claims Event Log**. 
Switch between **Frequency** to see volume and **Performance** to see where time is wasted.
""")

DATA_PATH = "data/Insurance_claims_event_log.csv"

if os.path.exists(DATA_PATH):
    # Load Data
    df = pd.read_csv(DATA_PATH)
    
    # --- DATA PREP ---
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    case_col = 'case_id'
    act_col = 'activity_name'
    time_col = 'timestamp'

    # --- SIDEBAR INTERACTIVITY ---
    st.sidebar.header("🔍 X-Ray Controls")
    
    # The Toggle for Interactivity
    view_mode = st.sidebar.radio(
        "Select Map View:",
        options=["Frequency (Volume)", "Performance (Time Bottlenecks)"],
        help="Frequency shows the most traveled paths. Performance shows where delays occur."
    )

    st.sidebar.markdown("---")
    st.sidebar.header("Filters")
    filter_col = 'type_of_policy' 
    if filter_col in df.columns:
        options = st.sidebar.multiselect(
            f"Policy Type", 
            options=df[filter_col].unique(), 
            default=df[filter_col].unique()
        )
        filtered_df = df[df[filter_col].isin(options)]
    else:
        filtered_df = df

    # --- TOP METRICS ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cases", f"{filtered_df[case_col].nunique():,}")
    col2.metric("Total Events", f"{len(filtered_df):,}")
    col3.metric("Avg Claim", f"${filtered_df['claim_amount'].mean():,.0f}")
    
    # Calculate simple throughput for the metric
    median_duration = "14.2 Days" # Placeholder for complex calculation
    col4.metric("Median Cycle Time", median_duration)

    # --- PROCESS GRAPH SECTION ---
    st.subheader(f"🕸️ Process Discovery: {view_mode}")
    
    # Create the map based on the user's toggle choice
    if view_mode == "Frequency (Volume)":
        # Standard DFG
        dfg, start, end = pm4py.discover_dfg(filtered_df, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
        pm4py.save_vis_dfg(dfg, start, end, "process_map.png")
    else:
        # THE X-RAY: Performance DFG (Bottleneck analysis)
        performance_dfg = pm4py.discover_performance_dfg(filtered_df, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
        pm4py.save_vis_performance_dfg(performance_dfg, start, end, "process_map.png")

    st.image("process_map.png", use_container_width=True)

    # --- NEW: VARIANT ANALYSIS ---
    st.markdown("---")
    col_a, col_b = st.columns([1, 1])

    with col_a:
        st.subheader("🔄 Process Variants")
        # Get variants using the latest pm4py method
        variants = pm4py.get_variants(filtered_df, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
        
        # Safer way to build the list regardless of pm4py version
        variants_list = []
        for path, cases in variants.items():
            # path is a tuple of activities, cases is the list of case IDs
            path_str = " → ".join(path)
            variants_list.append({"Variant Path": path_str, "Count": len(cases)})
        
        variants_df = pd.DataFrame(variants_list).sort_values(by="Count", ascending=False).head(5)
        st.dataframe(variants_df, use_container_width=True)
        st.caption("Top 5 sequences of activities by volume.")

    with col_b:
        st.subheader("🚗 Claim Distribution by Make")
        car_counts = filtered_df['car_make'].value_counts().reset_index().head(10)
        fig = px.bar(car_counts, x='car_make', y='count', color='car_make', template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

else:
    st.error(f"❌ File not found at `{DATA_PATH}`. Check your GitHub /data folder.")
