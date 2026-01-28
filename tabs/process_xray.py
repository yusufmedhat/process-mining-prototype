import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network
from engine.discovery import get_proprietary_dfg
import os

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 12)

    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 1. Initialize PyVis Network
    # 'directed=True' and high 'height' for visibility
    net = Network(height="500px", width="100%", directed=True, bgcolor="#ffffff", font_color="#000000")

    # 2. Add Nodes: White rectangles with black borders
    unique_activities = df['activity_name'].unique()
    for act in unique_activities:
        net.add_node(
            act, 
            label=act, 
            shape="box", 
            color={"background": "#FFFFFF", "border": "#000000"},
            borderWidth=1,
            font={"size": 14}
        )

    # 3. Add Edges: Uniform width, describe order
    for _, row in plot_df.iterrows():
        net.add_edge(
            row['activity_name'], 
            row['next_activity'], 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1
        )

    # 4. Force Horizontal Hierarchical Layout
    # This dictionary forces the "Serial" look (Left-to-Right)
    options = {
        "layout": {
            "hierarchical": {
                "enabled": True,
                "levelSeparation": 250,
                "nodeSpacing": 150,
                "direction": "LR",  # Left to Right
                "sortMethod": "directed"
            }
        },
        "interaction": {
            "hover": True,
            "navigationButtons": True,
            "tooltipDelay": 100
        },
        "physics": {"enabled": False} # Prevents jitter and refresh
    }
    net.set_options(f"""var options = {str(options)}""")

    # 5. Save and Render
    # We save to a temporary HTML file and display it in Streamlit
    try:
        path = "tmp_graph.html"
        net.save_graph(path)
        with open(path, 'r', encoding='utf-8') as f:
            html_data = f.read()
        components.html(html_data, height=550)
    except Exception as e:
        st.error(f"Error generating graph: {e}")