import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 10)

    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 1. Nodes: Rectangles, White Background, Black Border
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

    # 2. Edges: Uniform thickness (no noise), describing order
    edges = []
    for _, row in plot_df.iterrows():
        edges.append(Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=f"{int(row['frequency'])}",
            color="#999999",
            width=1 # Uniform width as requested
        ))

    # 3. CONFIG: This is what forces the Horizontal Plane
    config = Config(
        width=1200,
        height=400,
        directed=True,
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        physics=False, # STOP REFRESH: Locks nodes in place
        hierarchical=True,
        direction="LR", # LEFT TO RIGHT
        sortMethod="directed",
        levelSeparation=300, # Increases horizontal space
        nodeSpacing=150      # Increases vertical gap between parallel steps
    )

    # 4. Render
    try:
        # We use a static key to ensure Streamlit doesn't refresh the state on zoom
        agraph(nodes=nodes, edges=edges, config=config, key="process_mining_stable")
    except Exception:
        st.error("Visualization error. Please refresh the page.")