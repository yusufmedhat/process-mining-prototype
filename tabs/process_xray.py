import streamlit as st
from engine.discovery import get_proprietary_dfg

def render(df):
    st.header("🛣️ Process X-Ray: Execution Intelligence")
    
    with st.sidebar:
        st.subheader("Map Fine-Tuning")
        top_k = st.slider("Flow Complexity", 1, 50, 15)

    # 1. Get the Flow Data
    edges_df = get_proprietary_dfg(df)
    
    if not edges_df.empty:
        # Sort by most frequent paths
        plot_df = edges_df.sort_values("frequency", ascending=False).head(top_k)

        # 2. Build a clear DOT Graph (Left-to-Right Flow)
        dot = "digraph {\n"
        dot += '  rankdir=LR; node [shape=rect, style="rounded,filled", fillcolor="#E1F5FE", fontname="Arial"];\n'
        
        for _, row in plot_df.iterrows():
            source = str(row['activity_name'])
            target = str(row['next_activity'])
            freq = int(row['frequency'])
            
            # --- IMPROVED SCALING LOGIC ---
            # This caps the thickness at 5 and ensures it doesn't get "disgusting"
            scaled_width = min(5, max(1, freq / 100)) 
            
            # Add the connection with a thinner, cleaner arrow
            dot += f'  "{source}" -> "{target}" [label="{freq}", penwidth={scaled_width}, color="#444444", fontname="Arial"];\n'
        
        dot += "}"

        # 3. Render the Process Map
        st.subheader("Enterprise Process Flow")
        st.graphviz_chart(dot)
        
    else:
        st.warning("⚠️ The Engine could not find any process connections. Check your data columns.")