import streamlit as st
import pandas as pd
import polars as pl
import os
# Importing our custom modules
from tabs import process_xray, social_mining, case_explorer

st.set_page_config(layout="wide", page_title="Nestlé Process Excellence Hub", page_icon="🛡️")

# --- CEO-LEVEL DATA ENGINE ---
@st.cache_data(show_spinner=False)
def load_data_turbo():
    # Update path to point to the Turbo Parquet file
    path = "data/Insurance_claims_event_log.parquet"
    
    if os.path.exists(path):
        # Polars reads Parquet almost instantly because it doesn't have to parse text
        df_pl = pl.read_parquet(path)
        
        # Parquet files preserve datetime types, so no extra conversion needed!
        # Convert to Pandas for compatibility with existing UI components
        return df_pl.to_pandas()
    
    # Fallback to CSV if Parquet isn't found (Good for development)
    csv_path = "data/Insurance_claims_event_log.csv"
    if os.path.exists(csv_path):
        df_pl = pl.read_csv(csv_path).with_columns(
            pl.col("timestamp").str.to_datetime(strict=False)
        )
        return df_pl.to_pandas()
        
    return None

# --- UI EXECUTION ---
# This is the first thing the CEO sees - high-end polished spinner
with st.spinner("✨ Synchronizing Digital Twin Assets..."):
    df = load_data_turbo()

if df is not None:
    # --- STYLIZED SIDEBAR ---
    with st.sidebar:
        st.title("🛡️ Nestlé Hub")
        st.success("Digital Twin: Online")
        
        st.markdown("### Executive Dashboard")
        choice = st.radio(
            "Analysis Level", 
            ["🛣️ Process X-Ray", "👥 Social Mining", "🔍 Case Explorer"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        # Real-time KPIs calculated once at load
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Events", f"{len(df)//1000}k")
        with c2:
            st.metric("Cases", f"{df['case_id'].nunique():,}")
        
        st.caption("Engine: Polars v1.0 | Format: Parquet")

    # --- ROUTING ---
    if choice == "🛣️ Process X-Ray":
        process_xray.render(df)
    elif choice == "👥 Social Mining":
        social_mining.render(df)
    elif choice == "🔍 Case Explorer":
        case_explorer.render(df)
else:
    st.error("❌ Critical Failure: Data assets not found in /data directory.")