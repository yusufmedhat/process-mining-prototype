import streamlit as st
import pm4py
import plotly.figure_factory as ff
import pandas as pd

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar.expander("🛠️ X-Ray Fine-Tuning", expanded=True):
        top_k = st.slider("Variant Coverage (Top %)", 1, 100, 20)
        metric_type = st.radio("Analysis Lens", ["Frequency", "Performance"], horizontal=True)

    # --- 1. DATA RE-ENGINEERING (The Stability Layer) ---
    # We enforce string types for activities to prevent internal PM4PY IndexErrors
    proc_df = df.copy()
    proc_df['activity_name'] = proc_df['activity_name'].astype(str)
    proc_df['case_id'] = proc_df['case_id'].astype(str)
    
    # Map to XES Standard
    proc_df = proc_df.rename(columns={
        'case_id': 'case:concept:name',
        'activity_name': 'concept:name',
        'timestamp': 'time:timestamp'
    })
    proc_df['time:timestamp'] = pd.to_datetime(proc_df['time:timestamp'])

    # Convert to formal EventLog object (Essential for Performance Discovery)
    log_obj = pm4py.convert_to_event_log(proc_df)
    filtered_log = pm4py.filter_variants_top_k(log_obj, top_k)

    # --- 2. MULTI-MODAL ANALYSIS LAYOUT ---
    col_map, col_stats = st.columns([3, 1])

    with col_map:
        st.subheader("Process Digital Twin")
        try:
            if metric_type == "Frequency":
                dfg, start_act, end_act = pm4py.discover_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(
                    dfg, log=filtered_log, 
                    variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY,
                    parameters={"format": "svg", "start_activities": start_act, "end_activities": end_act}
                )
            else:
                # Performance Discovery
                dfg_perf = pm4py.discover_performance_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(
                    dfg_perf, log=filtered_log, 
                    variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE,
                    parameters={"format": "svg"}
                )
            st.graphviz_chart(gviz, use_container_width=True)
        except Exception as e:
            st.error(f"Visualization Engine Error: {e}")
            st.info("Try adjusting the Variant Coverage slider to refresh the map.")

    # --- 3. PERFORMANCE DISTRIBUTION (The 'State-of-the-Art' Context) ---
    with col_stats:
        st.subheader("⏱️ Throughput")
        case_durations = pm4py.get_all_case_durations(filtered_log)
        
        if case_durations:
            avg_days = sum(case_durations) / len(case_durations) / 86400
            st.metric("Avg. Cycle Time", f"{avg_days:.1f} Days")
            
            # Kernel Density Estimation (KDE) plot
            days_list = [d / 86400 for d in case_durations] 
            fig = ff.create_distplot([days_list], ['Days'], show_hist=False, show_rug=False)
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No duration data available for these variants.")

    # --- 4. TRACE AUDIT ---
    st.markdown("---")
    with st.expander("🔍 Trace Compliance Audit"):
        act_list = sorted(df['activity_name'].unique())
        selected_act = st.multiselect("Highlight cases containing:", act_list)
        if selected_act:
            # Use original DF for quick lookup
            matches = df[df['activity_name'].isin(selected_act)]['case_id'].unique()
            st.success(f"Found {len(matches)} cases following this specific pattern.")