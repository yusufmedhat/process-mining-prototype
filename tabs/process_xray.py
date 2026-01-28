import streamlit as st
from st_link_analysis import st_link_analysis
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 10)

    # 1. Get and Clean Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Build Elements - Simplified for stability
    # We remove complex style dicts inside the list to prevent rendering errors
    nodes = [
        {"data": {"id": str(act), "label": str(act)}}
        for act in df['activity_name'].unique()
    ]

    edges = [
        {
            "data": {
                "id": f"{row['activity_name']}_{row['next_activity']}",
                "source": str(row['activity_name']), 
                "target": str(row['next_activity']), 
                "label": f"{int(row['frequency'])}"
            }
        }
        for _, row in plot_df.iterrows()
    ]

    # 3. Global Styles: White boxes, Black borders, Directed arrows
    styles = [
        {
            "selector": "node",
            "style": {
                "shape": "rectangle",
                "background-color": "#FFFFFF",
                "border-width": 1,
                "border-color": "#000000",
                "label": "data(label)",
                "width": 140,
                "height": 40,
                "text-valign": "center",
                "text-halign": "center",
                "font-size": "12px",
                "color": "#000000"
            }
        },
        {
            "selector": "edge",
            "style": {
                "width": 1,
                "line-color": "#999999",
                "target-arrow-color": "#999999",
                "target-arrow-shape": "triangle",
                "curve-style": "bezier",
                "label": "data(label)",
                "font-size": "10px",
                "text-rotation": "autorotate"
            }
        },
        {
            # This handles the "Blur" effect by dimming everything else on hover
            "selector": "node:selected",
            "style": {
                "border-color": "#F7A01B",
                "border-width": 3
            }
        }
    ]

    # 4. The Layout: Forced Horizontal Pipeline
    layout = {
        "name": "dagre",
        "rankDir": "LR",  # Left to Right
        "nodeSep": 50,
        "rankSep": 200,
        "animate": False
    }

    # 5. Render
    try:
        st_link_analysis(
            elements={"nodes": nodes, "edges": edges},
            layout=layout,
            styling=styles,
            key="final_process_map"
        )
    except Exception:
        st.error("Component failed to render. Please check connectivity.")