import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Interactive Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 20)

    # 1. Get Flow Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Create Nodes
    unique_activities = df['activity_name'].unique()
    nodes = [
        Node(id=act, 
             label=act, 
             size=25, 
             shape="circularDot",
             color="#1f77b4") 
        for act in unique_activities
    ]

    # 3. Create Edges
    edges = [
        Edge(source=row['activity_name'], 
             target=row['next_activity'], 
             label=f"{int(row['frequency'])}") 
        for _, row in plot_df.iterrows()
    ]

    # 4. Configuration for Focus/Blur effect
    config = Config(
        width=1000,
        height=600,
        directed=True,
        physics=True,
        hierarchical=False,
        # This setting handles the "blur others" logic
        highlightDegree=1, 
        collapsible=False,
        nodeHighlightBehavior=True,
        linkHighlightBehavior=True,
        highlightColor="#F7A01B" # Color when focused
    )

    # 5. Render
    agraph(nodes=nodes, edges=edges, config=config)