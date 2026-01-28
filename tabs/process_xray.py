from engine.discovery import get_proprietary_dfg
from st_link_analysis import st_link_analysis # High-end interactive component

def render(df):
    # Run your proprietary engine
    edges_df = get_proprietary_dfg(df)
    
    # Prepare data for Interactive UI (Cytoscape format)
    nodes = [{"data": {"id": act, "label": act}} for act in df['activity_name'].unique()]
    edges = [
        {"data": {"source": row['activity_name'], "target": row['next_activity'], "label": f"{row['frequency']} hits"}}
        for _, row in edges_df.iterrows()
    ]
    
    # CEO Level Interactivity: Zoom, Drag, and Click
    st_link_analysis({"nodes": nodes, "edges": edges}, layout="cola")