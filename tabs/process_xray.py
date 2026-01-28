import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 12)

    # 1. Get Flow Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Nodes: Pure White, Black Border, Box Shape
    unique_activities = df['activity_name'].unique()
    nodes = [
        Node(
            id=act, 
            label=act, 
            shape="box",
            color="#FFFFFF",
            font={'color': '#000000', 'size': 14, 'face': 'Arial'},
            borderWidth=1
        ) 
        for act in unique_activities
    ]

    # 3. Edges: Uniform Gray Lines
    edges = [
        Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1
        ) 
        for _, row in plot_df.iterrows()
    ]

    # 4. Strict Horizontal Hierarchical Config
    # We use a nested dictionary for hierarchical to ensure the 'LR' command is respected.
    config = Config(
        width=1200,
        height=400,
        directed=True,
        physics=False,  # Essential to stop the refresh-on-zoom
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        hierarchical={
            "enabled": True,
            "levelSeparation": 250, # Space between steps
            "nodeSpacing": 100,      # Space between parallel steps
            "direction": "LR",       # Forced Left-to-Right
            "sortMethod": "directed" # Orders by process flow
        }
    )

    # 5. Render
    try:
        agraph(nodes=nodes, edges=edges, config=config)
    except Exception:
        st.error("Visualization error. Try reducing 'Flow Complexity' in the sidebar.")