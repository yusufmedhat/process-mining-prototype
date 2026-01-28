import streamlit as st
import pm4py
import plotly.figure_factory as ff
import pandas as pd

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    # 1. Advanced Configuration Sidebar
    with st.sidebar.expander("🛠️ X-Ray Fine-Tuning", expanded=True):
        top_k = st.slider("Variant Coverage (Top %)", 1, 100, 20)
        metric_type = st.radio("Analysis Lens", ["Frequency", "Performance"], horizontal=True)

    # --- THE STABILITY FIX: PRE-EMPTIVE STANDARDIZATION ---
    # We rename columns to PM4PY defaults to prevent KeyErrors in visualization
    event_log = df.copy()
    event_log = event_log.rename(columns={
        'case_id': 'case:concept:name',
        'activity_name': 'concept:name',
        'timestamp': 'time:timestamp'
    })
    # Ensure timestamp is datetime
    event_log['time:timestamp'] = pd.to_datetime(event_log['time:timestamp'])

    # 2. Optimized Data Filtering (Top K Variants)
    filtered_log = pm4py.filter_variants_top_k(event_log, top_k)

    # 3. Process Discovery & Dashboard Layout
    col_map, col_stats = st.columns([3, 1])

    with col_map:
        st.subheader("Interactive Process Map")
        if metric_type == "Frequency":
            dfg, start, end = pm4py.discover_dfg(filtered_log)
            gviz = pm4py.visualization.dfg.visualizer.apply(
                dfg, log=filtered_log, 
                variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY,
                parameters={"format": "svg"}
            )
        else:
            dfg_perf = pm4py.discover_performance_dfg(filtered_log)
            gviz = pm4py.visualization.dfg.visualizer.apply(
                dfg_perf, log=filtered_log, 
                variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE,
                parameters={"format": "svg"}
            )
        
        st.graphviz_chart(gviz, use_container_width=True)

    # 4. State-of-the-Art Statistical Context
    with col_stats:
        st.subheader("⏱️ Throughput")
        case_durations = pm4py.get_all_case_durations(filtered_log)
        
        if case_durations:
            avg_days = sum(case_durations) / len(case_durations) / 86400
            st.metric("Avg. Cycle Time", f"{avg_days:.1f} Days")
            
            # Distribution Plot (Density Curve)
            hist_data = [d / 86400 for d in case_durations] 
            fig = ff.create_distplot([hist_data], ['Days'], show_hist=False, show_rug=False)
            fig.update_layout(
                height=200, 
                margin=dict(l=0, r=0, t=0, b=0), 
                showlegend=False,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No duration data found.")

    # 5. Semantic Trace Filtering
    st.markdown("---")
    with st.expander("🔍 Trace Compliance Search"):
        act_list = sorted(df['activity_name'].unique())
        selected_act = st.multiselect("Find cases that passed through:", act_list)
        if selected_act:
            # Revert to original df for simple case-id matching
            matches = df[df['activity_name'].isin(selected_act)]['case_id'].unique()
            st.success(f"Identified {len(matches)} cases matching your criteria.")