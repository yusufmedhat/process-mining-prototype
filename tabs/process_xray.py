import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network
from engine.discovery import get_proprietary_dfg
import json

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 12)

    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 1. Initialize Network
    net = Network(height="500px", width="100%", directed=True, bgcolor="#ffffff")

    # 2. Add Nodes: Professional White/Black styling
    for act in df['activity_name'].unique():
        net.add_node(
            act, 
            label=act, 
            shape="box", 
            color={
                "background": "#FFFFFF", 
                "border": "#000000", 
                # Highlight turns border thicker black instead of orange
                "highlight": {"background": "#FFFFFF", "border": "#000000"},
                "hover": {"background": "#F0F2F6", "border": "#000000"}
            },
            font={"color": "#000000", "size": 14, "face": "Arial"},
            borderWidth=1
        )

    # 3. Add Edges
    for _, row in plot_df.iterrows():
        net.add_edge(
            str(row['activity_name']), 
            str(row['next_activity']), 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1,
            font={"size": 10, "align": "top"}
        )

    # 4. JSON-Safe Configuration: Tightened and Cleaned
    options = {
        "layout": {
            "hierarchical": {
                "enabled": True,
                "levelSeparation": 150, # Reduced from 300 to bring nodes closer
                "nodeSpacing": 100,     # Reduced from 150
                "direction": "LR",
                "sortMethod": "directed"
            }
        },
        "interaction": {
            "hover": True,
            "multiselect": False,
            "navigationButtons": False, # Removed the green buttons
            "tooltipDelay": 100
        },
        "edges": {
            "smooth": {"type": "cubicBezier", "forceDirection": "horizontal"},
            "color": {"inherit": False}
        },
        "physics": {"enabled": False}
    }
    
    net.set_options(json.dumps(options))

    # 5. Save and Render
    path = "tmp_graph.html"
    net.save_graph(path)
    
    with open(path, 'r', encoding='utf-8') as f:
        html_data = f.read()
    
    components.html(html_data, height=550)