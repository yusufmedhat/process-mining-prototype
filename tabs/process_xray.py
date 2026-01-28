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

    # 2. Add Nodes: White Rectangles
    for act in df['activity_name'].unique():
        net.add_node(
            act, 
            label=act, 
            shape="box", 
            color={"background": "#FFFFFF", "border": "#000000", "highlight": "#F7A01B"},
            font={"color": "#000000", "size": 14}
        )

    # 3. Add Edges: Uniform Gray
    for _, row in plot_df.iterrows():
        net.add_edge(
            str(row['activity_name']), 
            str(row['next_activity']), 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1
        )

    # 4. JSON-Safe Configuration
    # This forces the horizontal plane and enables the "focus" behavior
    options = {
        "layout": {
            "hierarchical": {
                "enabled": True,
                "levelSeparation": 300,
                "nodeSpacing": 150,
                "direction": "LR",
                "sortMethod": "directed"
            }
        },
        "interaction": {
            "hover": True,
            "multiselect": True,
            "navigationButtons": True
        },
        "nodes": {
            "borderWidth": 1,
            "borderWidthSelected": 2
        },
        "physics": {"enabled": False}
    }
    
    # Use json.dumps to ensure JS-compatible double quotes
    net.set_options(json.dumps(options))

    # 5. Save and inject custom "Blur" CSS
    path = "tmp_graph.html"
    net.save_graph(path)
    
    with open(path, 'r', encoding='utf-8') as f:
        html_data = f.read()
    
    # Optional: Display in Streamlit
    components.html(html_data, height=550)