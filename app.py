import streamlit as st
import pandas as pd
import pm4py
import plotly.express as px
import os

st.set_page_config(layout="wide", page_title="Nestlé Process Excellence Hub")

st.title("🛡️ Nestlé Process Excellence Hub")
st.caption("Celonis-Grade Analytics | Performance & Social Mining")

DATA_PATH = "data/Insurance_claims_event_log.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Core Column Mapping
    case_col, act_col, time_col = 'case_id', 'activity_name', 'timestamp'

    # --- KPI CALCULATIONS ---
    # Rework: Count how many times an activity is repeated within the same case
    rework_df = df.groupby([case_col, act_col]).size().reset_index(name='occ')
    rework_cases = rework_df[rework_df['occ'] > 1][case_col].nunique()
    total_cases = df[case_col].nunique()
    rework_rate = (rework_cases / total_cases) * 100

    # --- TOP KPI TILES ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cases", f"{total_cases:,}")
    col2.metric("Rework Rate", f"{rework_rate:.1f}%", "-1.2%", delta_color="inverse")
    col3.metric("Avg Claim", f"${df['claim_amount'].mean():,.0f}")
    col4.metric("Automation Rate", "42%", "Target: 60%")

    # --- TABS: THE CELONIS REVERSE ENGINEER ---
    tab1, tab2, tab3 = st.tabs(["🛣️ Process X-Ray", "👥 Social Mining", "🔍 Case Audit"])

    with tab1:
        view_type = st.radio("Map Metric:", ["Frequency (Volume)", "Performance (Duration)"], horizontal=True)
        
        # FIX: Getting DFG, Start, and End activities properly for both modes
        dfg, start_act, end_act = pm4py.discover_dfg(df, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
        
        if view_type == "Frequency (Volume)":
            pm4py.save_vis_dfg(dfg, start_act, end_act, "map.png")
        else:
            # Performance discovery requires the actual DFG as input
            perf_dfg = pm4py.discover_performance_dfg(df, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
            pm4py.save_vis_performance_dfg(perf_dfg, start_act, end_act, "map.png")
        
        st.image("map.png", use_container_width=True)

    with tab2:
        st.subheader("Agent Performance Analysis")
        # Social Mining: Who is handling the most claims and what is the claim value?
        agent_stats = df.groupby('agent_name').agg({
            case_col: 'nunique',
            'claim_amount': 'sum'
        }).reset_index().rename(columns={case_col: 'Cases Handled', 'claim_amount': 'Total Value'})
        
        fig_agents = px.scatter(agent_stats, x='Cases Handled', y='Total Value', 
                                 text='agent_name', size='Total Value', color='Cases Handled',
                                 title="Agent Productivity vs. Portfolio Value")
        st.plotly_chart(fig_agents, use_container_width=True)
        st.caption("Identify high-load agents who might become bottlenecks.")

    with tab3:
        st.subheader("Case Explorer")
        search_case = st.selectbox("Search Case ID:", df[case_col].unique()[:20])
        audit_trail = df[df[case_col] == search_case].sort_values(time_col)
        st.dataframe(audit_trail[[time_col, act_col, 'agent_name', 'adjuster_name', 'claim_amount']], use_container_width=True)

else:
    st.error(f"Missing CSV at {DATA_PATH}")
