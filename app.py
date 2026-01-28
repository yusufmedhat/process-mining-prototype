import streamlit as st
import pandas as pd
import pm4py
import plotly.express as px
import os

st.set_page_config(layout="wide", page_title="Nestlé Process Excellence Hub")

st.title("🛡️ Nestlé Process Excellence Hub")
st.caption("Celonis-Grade Analytics | Interactive Digital Twin")

DATA_PATH = "data/Insurance_claims_event_log.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Define Keys
    case_col, act_col, time_col = 'case_id', 'activity_name', 'timestamp'

    # --- SIDEBAR: CELONIS CONTROLS ---
    st.sidebar.header("🕹️ Control Tower")
    top_k = st.sidebar.slider("Process Path Coverage (Top Variants)", 1, 100, 20)
    view_type = st.sidebar.radio("Map Metric:", ["Frequency (Volume)", "Performance (Duration)"])

    # --- KPI TILES ---
    total_cases = df[case_col].nunique()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Cases", f"{total_cases:,}")
    col2.metric("Avg Claim", f"${df['claim_amount'].mean():,.0f}")
    col3.metric("Standardization", f"{(top_k/100)*100:.0f}%", "Target: 80%")

    # --- MAIN TABS ---
    tab1, tab2, tab3 = st.tabs(["🛣️ Process X-Ray", "👥 Social Mining", "🔍 Case Audit"])

    with tab1:
        st.subheader(f"Dynamic {view_type} Map")
        
        # 1. Filter variants
        filtered_log = pm4py.filter_variants_top_k(df, top_k, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
        
        # 2. Setup Parameters to prevent KeyErrors
        # This tells pm4py exactly which columns to use for visualization
        params = {
            "case_id_key": case_col,
            "activity_key": act_col,
            "timestamp_key": time_col
        }

        if view_type == "Frequency (Volume)":
            dfg, start_act, end_act = pm4py.discover_dfg(filtered_log, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
            gviz = pm4py.visualization.dfg.visualizer.apply(dfg, log=filtered_log, variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY, parameters=params)
        else:
            dfg_perf = pm4py.discover_performance_dfg(filtered_log, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
            gviz = pm4py.visualization.dfg.visualizer.apply(dfg_perf, log=filtered_log, variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE, parameters=params)
        
        st.graphviz_chart(gviz)
        st.caption(f"🔍 Currently showing the top {top_k} process variants.")

    with tab2:
        st.subheader("Agent Productivity")
        agent_stats = df.groupby('agent_name').agg({case_col: 'nunique', 'claim_amount': 'sum'}).reset_index()
        fig = px.scatter(agent_stats, x=case_col, y='claim_amount', text='agent_name', size='claim_amount', color=case_col)
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Case Explorer")
        search_case = st.selectbox("Search Case ID:", df[case_col].unique()[:20])
        st.dataframe(df[df[case_col] == search_case].sort_values(time_col), use_container_width=True)

else:
    st.error("Missing CSV. Check your /data folder.")
