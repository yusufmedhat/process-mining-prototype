import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        top_k = st.slider("Flow Complexity", 1, 50, 15)

    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Nodes: White background, thin black borders, clean text
    unique_activities = df['activity_name'].unique()
    nodes = [
        Node(
            id=act, 
            label=act, 
            shape="box",
            color={
                "background": "#FFFFFF",
                "border": "#000000",
                "highlight": {"background": "#FFFFFF", "border": "#F7A01B"}
            },
            font={'color': '#000000', 'size': 14, 'face': 'Arial'},
            borderWidth=1
        ) 
        for act in unique_activities
    ]

    # 3. Edges: Thinner, less intrusive lines
    edges = [
        Edge(
            source=row['activity_name'], 
            target=row['next_activity'], 
            label=f"{int(row['frequency'])}",
            color="#CCCCCC", # Light grey to reduce 'noise'
            arrowStrikethrough=False
        ) 
        for _, row in plot_df.iterrows()
    ]

    # 4. Config: STOP THE REFRESH
    config = Config(
        width=1000,
        height=600,
        directed=True,
        # Set physics to False to stop the jitter/rerun loop on click/zoom
        physics=False, 
        hierarchical=False,
        nodeHighlightBehavior=True,
        highlightDegree=1,
        linkHighlightBehavior=True,
        # crucial for performance and preventing refresh loops:
        staticGraph=False 
    )

    # 5. Static Render
    # Using a static key prevents Streamlit from re-initializing the state on every interaction
    agraph(nodes=nodes, edges=edges, config=config, key="process_mining_graph")