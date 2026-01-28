import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        st.subheader("Map Controls")
        # Keep complexity low to ensure a clean horizontal line
        top_k = st.slider("Flow Complexity", 1, 50, 12)

    # 1. Get Flow Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    # Sort by frequency to ensure the "main" path is prioritized in the layout
    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Nodes: White background, thin black borders, horizontal alignment
    unique_activities = df['activity_name'].unique()
    nodes = []
    for act in unique_activities:
        nodes.append(Node(
            id=act, 
            label=act, 
            shape="box",
            color="#FFFFFF",
            font={'color': '#000000', 'size': 14},
            borderWidth=1
        ))

    # 3. Edges: Uniform width, describing the order
    edges = []
    for _, row in plot_df.iterrows():
        edges.append(Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=f" {int(row['frequency'])} ",
            color="#999999",
            width=1, # Uniform width as requested
            arrowStrikethrough=False
        ))

    # 4. Hierarchical Config: Forces the "Serial" horizontal plane
    config = Config(
        width=1200,
        height=400,
        directed=True,
        physics=False, # STOP REFRESH: Locked positions
        hierarchical=True, # Forces order
        direction="LR", # Left to Right horizontal plane
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        staticGraph=False
    )

    # 5. Render with fixed key to prevent state loss/refresh on zoom
    agraph(nodes=nodes, edges=edges, config=config, key="serial_process_map")