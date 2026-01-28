import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        st.subheader("Map Controls")
        top_k = st.slider("Flow Complexity", 1, 50, 15)

    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 1. Professional Nodes
    nodes = []
    for act in df['activity_name'].unique():
        nodes.append(Node(
            id=act, 
            label=act, 
            shape="box",
            color="#FFFFFF",
            font={'color': '#000000', 'size': 12},
            borderWidth=1
        ))

    # 2. Curved Edges for Readability
    edges = []
    for _, row in plot_df.iterrows():
        edges.append(Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1,
            # This makes the lines curve around each other
            type="CURVED_SMOOTH" 
        ))

    # 3. Spaced-Out Config (Non-Hierarchical)
    config = Config(
        width=1200,   # Increased width
        height=800,   # Increased height for better vertical spread
        directed=True,
        nodeHighlightBehavior=True,
        highlightColor="#F7A01B",
        # Switching physics to BarnesHut to force nodes to spread out
        physics=True, 
        solver="barnesHut",
        barnesHut={
            "gravitationalConstant": -3000, # Stronger "push" to prevent overlapping
            "centralGravity": 0.3,
            "springLength": 200,            # Longer connections for readability
            "springConstant": 0.05
        },
        # Disables the "dancing" after it finds its spot
        stabilization=True 
    )

    try:
        agraph(nodes=nodes, edges=edges, config=config, key="spaced_process_map")
    except Exception:
        # Fallback to simple render if keys conflict
        agraph(nodes=nodes, edges=edges, config=config)