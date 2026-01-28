import streamlit as st
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        st.subheader("Map Controls")
        top_k = st.slider("Flow Complexity", 1, 50, 15)

    # 1. Get Flow Data
    edges_df = get_proprietary_dfg(df)
    if edges_df.empty:
        st.warning("No data found.")
        return

    # Take the top paths to keep the "Serial" look clean
    plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

    # 2. Build the DOT string for a horizontal serial flow
    # rankdir=LR forces the Left-to-Right orientation
    dot = """
    digraph {
        rankdir=LR;
        node [shape=rect, style="filled", fillcolor="white", color="black", fontname="Arial", fontsize="10"];
        edge [color="#999999", fontname="Arial", fontsize="8"];
    """

    for _, row in plot_df.iterrows():
        source = str(row['activity_name'])
        target = str(row['next_activity'])
        freq = int(row['frequency'])
        
        # Adding the connection
        dot += f'    "{source}" -> "{target}" [label=" {freq} "];\n'

    dot += "}"

    # 3. Render the Map
    # st.graphviz_chart is native to Streamlit: No refreshing, no crashing.
    st.subheader("Enterprise Process Flow")
    st.graphviz_chart(dot, use_container_width=True)