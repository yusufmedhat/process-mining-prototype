import streamlit as st
import pandas as pd
import os
# Importing our custom modules from the /tabs folder
from tabs import process_xray, social_mining, case_explorer

st.set_page_config(layout="wide", page_title="Nestlé Process Excellence Hub")

# Shared Data Loading with Caching for Speed
@st.cache_data
def load_data():
    path = "data/Insurance_claims_event_log.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        return df
    return None

df = load_data()

if df is not None:
    st.sidebar.title("🛡️ Nestlé Hub")
    st.sidebar.info("Celonis-Grade Digital Twin")
    
    # Navigation Menu
    choice = st.sidebar.radio(
        "Analysis Level", 
        ["🛣️ Process X-Ray", "👥 Social Mining", "🔍 Case Explorer"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.caption("Data Source: Insurance_claims_event_log.csv")

    # Route to the correct file
    if choice == "🛣️ Process X-Ray":
        process_xray.render(df)
    elif choice == "👥 Social Mining":
        social_mining.render(df)
    elif choice == "🔍 Case Explorer":
        case_explorer.render(df)
else:
    st.error("❌ CSV Data not found. Please ensure the file is in the /data folder.")