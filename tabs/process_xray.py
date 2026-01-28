import streamlit as st
import pm4py
import plotly.figure_factory as ff
import pandas as pd

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar.expander("🛠️ X-Ray Fine-Tuning", expanded=True):
        top_k = st.slider("Variant Coverage (Top %)", 1, 100, 20)
        metric_type = st.radio("Analysis Lens", ["Frequency", "Performance"], horizontal=True)

    # --- 1. TYPE-SAFETY & STANDARDIZATION ---
    event_log = df.copy()
    # Convert all activities to strings to prevent the index [0] error
    event_log['activity_name'] = event_log['activity_name'].astype(str)
    
    event_log = event_log.rename(columns={
        'case_id': 'case:concept:name',
        'activity_name': 'concept:name',
        'timestamp': 'time:timestamp'
    })
    event_log['time:timestamp'] = pd.to_datetime(event_log['time:timestamp'])

    # --- 2. LOG CONVERSION (The Stability Secret) ---
    # Converting from DataFrame to EventLog object explicitly fixes visualizer bugs
    log_obj = pm4py.convert_to_event_log(event_log)
    filtered_log = pm4py.filter_variants_top_k(log_obj, top_k)

    col_map, col_stats = st.columns([3, 1])

    with col_map:
        st.subheader("Interactive Process Map")
        try:
            if metric_type == "Frequency":
                dfg, start, end = pm4py.discover_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(
                    dfg, log=filtered_log, 
                    variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY,
                    parameters={"format": "svg", "start_activities": start, "end_activities": end}
                )
            else:
                # Performance discovery requires the log for duration calculations
                dfg_perf = pm4py.discover_performance_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(
                    dfg_perf, log=filtered_log, 
                    variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE,
                    parameters={"format": "svg"}
                )
            st.graphviz_chart(gviz, use_container_width=True)
        except Exception as e:
            st.error(f"Visualization Error: {e}")
            st.info("Try increasing Variant Coverage to include more data points.")

    # --- 3. DURATION DISTRIBUTION ---
    with col_stats:
        st.subheader("⏱️ Throughput")
        case_durations = pm4py.get_all_case_durations(filtered_log)
        
        if case_durations:
            avg_days = sum(case_durations) / len(case_durations) / 86400
            st.metric("Avg. Cycle Time", f"{avg_days:.1f} Days")
            
            days_list = [d / 86400 for d in case_durations] 
            fig = ff.create_distplot([days_list], ['Days'], show_hist=False, show_rug=False)
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("Insufficient duration data.")

    # --- 4. DATA QUALITY AUDIT ---
    st.markdown("---")
    with st.expander("📋 Process Health Audit"):
        rework_perc = (len(df) - len(df.drop_duplicates([ 'case_id', 'activity_name' ]))) / len(df) * 100
        c1, c2, c3 = st.columns(3)
        c1.write(f"**Total Events:** {len(df):,}")
        c2.write(f"**Unique Activities:** {df['activity_name'].nunique()}")
        c3.write(f"**Rework Density:** {rework_perc:.1f}%")