import streamlit as st
from st_link_analysis import st_link_analysis
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 15)

    # 1. Get and Clean Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Build Elements with Strict Styling
    # We use 'label' for the text and 'id' for the connection
    nodes = [
        {
            "data": {"id": str(act), "label": str(act)},
            "style": {
                "background-color": "#FFFFFF",
                "shape": "rectangle",
                "border-width": 1,
                "border-color": "#000000",
                "width": 150,
                "height": 40,
                "text-valign": "center",
                "text-halign": "center",
                "color": "#000000"
            }
        }
        for act in df['activity_name'].unique()
    ]

    edges = [
        {
            "data": {
                "source": str(row['activity_name']), 
                "target": str(row['next_activity']), 
                "label": f"{int(row['frequency'])}"
            }
        }
        for _, row in plot_df.iterrows()
    ]

    # 3. The Layout: 'klay' or 'dagre' are best for Horizontal Serial flows
    # These layout engines respect the "order" of the process
    layout = {
        "name": "dagre",
        "rankDir": "LR", # Forced Left-to-Right
        "nodeSep": 50,
        "rankSep": 200
    }

    # 4. Render with Interactive Focus
    st.subheader("Interactive Serial Flow")
    st_link_analysis(
        elements={"nodes": nodes, "edges": edges},
        layout=layout,
        height=500
    )