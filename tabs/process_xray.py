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

    # 1. Clean Nodes: White background, thin black border
    unique_activities = df['activity_name'].unique()
    nodes = []
    for act in unique_activities:
        nodes.append(Node(
            id=act,
            label=act,
            shape="box",
            size=20,
            color="#FFFFFF", # White background
            borderWidth=1,
            labelHighlightBold=True
        ))

    # 2. Clean Edges: Light grey lines
    edges = []
    for _, row in plot_df.iterrows():
        edges.append(Edge(
            source=row['activity_name'],
            target=row['next_activity'],
            label=str(int(row['frequency'])),
            color="#CCCCCC", # Light grey noise reduction
            width=1
        ))

    # 3. Stable Configuration: Disabling physics prevents the click/zoom refresh
    config = Config(
        width=1000,
        height=600,
        directed=True,
        nodeHighlightBehavior=True,
        highlightColor="#F7A01B",
        # Disabling physics stops the jitter and reruns
        physics=False,
        hierarchical=False,
        collapsible=False
    )

    # 4. Render with Error Handling
    try:
        agraph(nodes=nodes, 
               edges=edges, 
               config=config)
    except Exception as e:
        st.error("Visualization error. Falling back to static map.")
        # Fallback if agraph fails again
        st.write("Current Edge Data:", plot_df)