import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 12)

    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 1. CLEAN NODES: White, Black Border, Box Shape
    nodes = []
    for act in df['activity_name'].unique():
        nodes.append(Node(
            id=act, 
            label=act, 
            shape="box",
            color="#FFFFFF",
            font={'color': '#000000', 'size': 14},
            borderWidth=1
        ))

    # 2. CLEAN EDGES: Uniform width, describing order
    edges = []
    for _, row in plot_df.iterrows():
        edges.append(Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1 # Keep it thin to avoid the blobs from your screenshot
        ))

    # 3. THE FIX: Strict Horizontal Configuration
    # We use a simplified config and disable physics entirely to prevent the refresh loop.
    config = Config(
        width=1200,
        height=400,
        directed=True,
        physics=False,  # Locks nodes so zooming/clicking doesn't trigger refresh
        hierarchical=True,
        direction="LR", # Forced Left-to-Right
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        highlightColor="#F7A01B",
        # Explicit spacing to ensure it stretches horizontally
        levelSeparation=300,
        nodeSpacing=150
    )

    # 4. RENDER with Error Catching
    try:
        # Use a stable key to prevent Streamlit from resetting the component
        agraph(nodes=nodes, edges=edges, config=config, key="process_mining_horizontal")
    except Exception as e:
        st.error(f"Component rendering failed. Falling back to static view.")
        st.graphviz_chart(generate_fallback_dot(plot_df))

def generate_fallback_dot(df):
    """If the interactive graph fails, this provides a clean horizontal fallback."""
    dot = "digraph { rankdir=LR; node [shape=rect, style=filled, fillcolor=white, color=black]; "
    for _, row in df.iterrows():
        dot += f'"{row["activity_name"]}" -> "{row["next_activity"]}" [label="{int(row["frequency"])}"]; '
    dot += "}"
    return dot