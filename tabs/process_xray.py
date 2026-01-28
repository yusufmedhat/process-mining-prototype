import streamlit as st
from st_link_analysis import st_link_analysis  # <--- THIS IS THE MISSING LINE
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    # 1. SIDEBAR CONTROLS
    with st.sidebar:
        st.subheader("Map Fine-Tuning")
        top_k = st.slider("Variant Coverage (%)", 1, 100, 20)

    # 2. RUN PROPRIETARY ENGINE
    # This calls your Polars-based logic from engine/discovery.py
    edges_df = get_proprietary_dfg(df)
    
    if not edges_df.empty:
        # Prepare Interactive Graph Data
        # We limit the edges based on the top_k slider for performance
        limit = int(len(edges_df) * (top_k / 100))
        plot_df = edges_df.head(limit)

        nodes = [{"data": {"id": act, "label": act}} for act in df['activity_name'].unique()]
        edges = [
            {
                "data": {
                    "source": row['activity_name'], 
                    "target": row['next_activity'], 
                    "label": f"{int(row['frequency'])} hits"
                }
            }
            for _, row in plot_df.iterrows()
        ]
        
        # 3. RENDER INTERACTIVE MAP
        st.subheader("Interactive Process Twin")
        st_link_analysis({"nodes": nodes, "edges": edges}, layout="cola")
    else:
        st.warning("No process paths found for the current selection.")