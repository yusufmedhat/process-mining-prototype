import streamlit as st
import pm4py
import plotly.figure_factory as ff
import pandas as pd

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    # 1. SIDEBAR FILTERS (The Input)
    with st.sidebar.expander("🛠️ Filter & Variant Control", expanded=True):
        top_k = st.slider("Variant Coverage (Top %)", 1, 100, 20)
        metric_type = st.radio("Analysis Lens", ["Frequency", "Performance"], horizontal=True)
        # Added a time filter to show reactivity
        min_date = df['timestamp'].min().date()
        max_date = df['timestamp'].max().date()
        date_range = st.date_input("Time Window", [min_date, max_date])

    # 2. DATA PIPELINE (The "Reactive" Step)
    # We create a fresh filtered_df every time a widget changes
    mask = (df['timestamp'].dt.date >= date_range[0]) & (df['timestamp'].dt.date <= date_range[1])
    working_df = df[mask].copy()

    # Standardize column names for PM4PY
    working_df = working_df.rename(columns={
        'case_id': 'case:concept:name',
        'activity_name': 'concept:name',
        'timestamp': 'time:timestamp'
    })
    
    # Convert to EventLog and filter by Top K Variants
    log_obj = pm4py.convert_to_event_log(working_df)
    filtered_log = pm4py.filter_variants_top_k(log_obj, top_k)

    # 3. STATS CALCULATION (Now using the filtered_log!)
    case_durations = pm4py.get_all_case_durations(filtered_log)
    
    col_map, col_stats = st.columns([3, 1])

    with col_stats:
        st.subheader("⏱️ Throughput")
        if case_durations:
            avg_sec = sum(case_durations) / len(case_durations)
            # Logic to switch units based on scale
            if avg_sec > 86400:
                st.metric("Avg. Cycle Time", f"{avg_sec/86400:.2f} Days")
            elif avg_sec > 3600:
                st.metric("Avg. Cycle Time", f"{avg_sec/3600:.1f} Hours")
            else:
                st.metric("Avg. Cycle Time", f"{avg_sec:.0f} Seconds")
            
            # Reactive KDE Plot
            days_list = [d / 86400 for d in case_durations] 
            fig = ff.create_distplot([days_list], ['Days'], show_hist=False, show_rug=False)
            fig.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
                              paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data in range.")

    with col_map:
        st.subheader("Process Digital Twin")
        try:
            if metric_type == "Frequency":
                dfg, start, end = pm4py.discover_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(dfg, log=filtered_log, variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY)
            else:
                dfg_perf = pm4py.discover_performance_dfg(filtered_log)
                gviz = pm4py.visualization.dfg.visualizer.apply(dfg_perf, log=filtered_log, variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE)
            st.graphviz_chart(gviz, use_container_width=True)
        except Exception:
            st.info("Simplifying map to handle variant density...")
            heu_net = pm4py.discover_heuristics_net(filtered_log)
            gviz = pm4py.visualization.heuristics_net.visualizer.apply(heu_net)
            st.graphviz_chart(gviz, use_container_width=True)