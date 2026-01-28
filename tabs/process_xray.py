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

    # 1. CLEAN NODES: White, Black Border, Rectangles
    nodes = []
    for act in df['activity_name'].unique():
        nodes.append(Node(
            id=act, 
            label=act, 
            shape="box",
            color="#FFFFFF", # White background
            borderWidth=1,   # Thin black border (default is black)
            font={'color': '#000000'}
        ))

    # 2. CLEAN EDGES: Uniform width, grey color
    edges = []
    for _, row in plot_df.iterrows():
        edges.append(Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=str(int(row['frequency'])),
            color="#999999",
            width=1
        ))

    # 3. STABLE HIERARCHICAL CONFIG
    # We remove 'staticGraph' and 'physics' conflicts to stop the TypeError
    config = Config(
        width=1000,
        height=400,
        directed=True,
        hierarchical=True,
        direction="LR",      # Left to Right
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        highlightColor="#F7A01B",
        physics=False        # Essential to stop the zoom-refresh loop
    )

    # 4. RENDER
    # If the key "serial_process_map" still causes a TypeError, 
    # remove it and let Streamlit handle the ID automatically.
    try:
        agraph(nodes=nodes, edges=edges, config=config)
    except Exception as e:
        st.error("Visualization Component Error. Checking data types...")
        st.write("Ensuring all IDs are strings:", [type(n.id) for n in nodes])