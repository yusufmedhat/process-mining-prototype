import streamlit as st

def render(df):
    st.header("🔍 Individual Case Explorer")
    
    case_ids = df['case_id'].unique()
    selected_case = st.selectbox("Search and select a Case ID to audit:", case_ids)
    
    if selected_case:
        case_data = df[df['case_id'] == selected_case].sort_values('timestamp')
        
        # Financial Summary for the specific case
        total_val = case_data['claim_amount'].iloc[0] if not case_data.empty else 0
        st.metric("Total Claim Value", f"${total_val:,.2f}")
        
        st.subheader("Process History (Audit Trail)")
        st.table(case_data[['timestamp', 'activity_name', 'agent_name', 'adjuster_name']])