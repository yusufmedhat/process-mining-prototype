import streamlit as st
from engine.discovery import get_proprietary_dfg # Your new engine

def render(df):
    # CEO-Level: Set default to 10 so it loads instantly
    with st.sidebar:
        st.subheader("Map Controls")
        top_k = st.slider("Process Complexity", 1, 100, 10) 
        
    # ONLY calculate the map when needed
    with st.status("Analysing Traces...", expanded=False) as status:
        st.write("Extracting Directly-Follows relationships...")
        # Use your Polars-based engine for speed
        edges_df = get_proprietary_dfg(df)
        status.update(label="Analysis Complete!", state="complete", expanded=False)
    # Run your proprietary engine
    edges_df = get_proprietary_dfg(df)
    
    # Prepare data for Interactive UI (Cytoscape format)
    nodes = [{"data": {"id": act, "label": act}} for act in df['activity_name'].unique()]
    edges = [
        {"data": {"source": row['activity_name'], "target": row['next_activity'], "label": f"{row['frequency']} hits"}}
        for _, row in edges_df.iterrows()
    ]
    
    # CEO Level Interactivity: Zoom, Drag, and Click
    st_link_analysis({"nodes": nodes, "edges": edges}, layout="cola")