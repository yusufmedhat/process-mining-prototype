import streamlit as st
import pm4py
import plotly.figure_factory as ff
import pandas as pd

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar.expander("🛠️ X-Ray Fine-Tuning", expanded=True):
        top_k = st.slider("Variant Coverage (Top %)", 1, 100, 20)
        metric_type = st.radio("Analysis Lens", ["Frequency", "Performance"], horizontal=True)

    # --- 1. DATA STABILIZATION (Standardizing for PM4PY) ---
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

    # Convert and Filter
    log_obj = pm4py.convert_to_event_log(proc_df)
    filtered_log = pm4py.filter_variants_top_k(log_obj, top_k)

    # --- 2. MULTI-MODAL ANALYSIS LAYOUT ---
    col_map, col_stats = st.columns([3, 1])

    with col_map:
        st.subheader("Process Digital Twin")
        try:
            # We use the DIRECT pm4py discover and view functions 
            # These are the most stable across all versions (2.0 to 3.0+)
            if metric_type == "Frequency":
                dfg, start_act, end_act = pm4py.discover_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(
                    dfg, log=filtered_log, 
                    variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY,
                    parameters={"format": "svg"}
                )
            else:
                # Performance view - using the performance DFG discovery directly
                dfg_perf = pm4py.discover_performance_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(
                    dfg_perf, log=filtered_log, 
                    variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE,
                    parameters={"format": "svg"}
                )
            st.graphviz_chart(gviz, use_container_width=True)
            
        except Exception as e:
            st.warning("Switching to Heuristic Map due to complexity...")
            # Fallback for complex performance logs
            heu_net = pm4py.discover_heuristics_net(filtered_log)
            gviz = pm4py.visualization.heuristics_net.visualizer.apply(heu_net, parameters={"format": "svg"})
            st.graphviz_chart(gviz, use_container_width=True)

    # --- 3. THROUGHPUT DISTRIBUTION (The 'State-of-the-Art' Context) ---
    with col_stats:
        st.subheader("⏱️ Throughput")
        case_durations = pm4py.get_all_case_durations(filtered_log)
        
        if case_durations:
            avg_days = sum(case_durations) / len(case_durations) / 86400
            st.metric("Avg. Cycle Time", f"{avg_days:.1f} Days")
            
            # Density Distribution
            days_list = [d / 86400 for d in case_durations] 
            fig = ff.create_distplot([days_list], ['Days'], show_hist=False, show_rug=False)
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)