import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Interactive Intelligence")
    
    with st.sidebar:
        st.subheader("Map Controls")
        top_k = st.slider("Flow Complexity", 1, 50, 20)

    # 1. Get Flow Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Create Nodes - Updated to Professional Rectangles
    # We use 'box' shape and soft colors to match the previous professional look
    unique_activities = df['activity_name'].unique()
    nodes = [
        Node(
            id=act, 
            label=act, 
            size=20, 
            shape="box", # Rectangle shape
            color="#F0F2F6", # Soft grey background
            font={'color': '#262730', 'size': 14, 'face': 'Arial'},
            borderWidth=2
        ) 
        for act in unique_activities
    ]

    # 3. Create Edges
    edges = [
        Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=f" {int(row['frequency'])} ",
            color="#6c757d",
            arrowStrikethrough=False
        ) 
        for _, row in plot_df.iterrows()
    ]

    # 4. Optimized Configuration
    config = Config(
        width=1000,
        height=600,
        directed=True,
        # Physics is set to False or 'stabled' to prevent constant refreshing/movement
        physics=True, 
        hierarchical=False,
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        highlightColor="#F7A01B", # Highlight color (Orange)
        # This helps stop the "jitter" that causes reruns
        minVelocity=0.75,
    )

    # 5. Render - We wrap it to handle the return value without refreshing logic
    return_value = agraph(nodes=nodes, edges=edges, config=config)