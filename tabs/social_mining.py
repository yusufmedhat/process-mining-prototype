import streamlit as st
import plotly.express as px

def render(df):
    st.header("👥 Social Mining & Productivity")
    
    # Calculate Agent KPIs
    agent_stats = df.groupby('agent_name').agg({
        'case_id': 'nunique',
        'claim_amount': 'sum'
    }).reset_index().rename(columns={'case_id': 'Cases Handled', 'claim_amount': 'Total Portfolio Value'})

    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig = px.scatter(agent_stats, x='Cases Handled', y='Total Portfolio Value', 
                         text='agent_name', size='Cases Handled', color='Total Portfolio Value',
                         title="Agent Productivity vs. Financial Exposure",
                         color_continuous_scale=px.colors.sequential.Viridis)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Performers")
        st.dataframe(agent_stats.sort_values('Cases Handled', ascending=False).head(10), hide_index=True)