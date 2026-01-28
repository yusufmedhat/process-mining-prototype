import streamlit as st
import pm4py

def render(df):
    st.header("🛣️ Process X-Ray Discovery")
    st.markdown("Visualize the flow of claims and identify bottlenecks.")
    
    # Column mapping
    case_col, act_col, time_col = 'case_id', 'activity_name', 'timestamp'
    
    # Controls for the Map
    col1, col2 = st.columns([1, 1])
    with col1:
        top_k = st.slider("Path Coverage (Top Variants)", 1, 100, 20, help="Lower = Happy Path, Higher = Spaghetti/Complexity")
    with col2:
        view_type = st.radio("Map Metric:", ["Frequency (Volume)", "Performance (Duration)"], horizontal=True)

    # Filtering the log
    filtered_log = pm4py.filter_variants_top_k(df, top_k, case_id_key=case_col, activity_key=act_col, timestamp_key=time_col)
    
    params = {
        "case_id_key": case_col,
        "activity_key": act_col,
        "timestamp_key": time_col
    }

    # Generate Graphviz Object
    if view_type == "Frequency (Volume)":
        dfg, start, end = pm4py.discover_dfg(filtered_log, **params)
        gviz = pm4py.visualization.dfg.visualizer.apply(dfg, log=filtered_log, variant=pm4py.visualization.dfg.visualizer.Variants.FREQUENCY, parameters=params)
    else:
        dfg_perf = pm4py.discover_performance_dfg(filtered_log, **params)
        gviz = pm4py.visualization.dfg.visualizer.apply(dfg_perf, log=filtered_log, variant=pm4py.visualization.dfg.visualizer.Variants.PERFORMANCE, parameters=params)
    
    # Render Interactive SVG
    st.graphviz_chart(gviz)
    st.caption("🔍 Hint: Zoom in to see specific activity names and transition times.")