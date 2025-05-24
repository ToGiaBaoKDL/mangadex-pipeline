# app.py
import streamlit as st
from src.dashboard.core.components.sidebar import render_sidebar
from src.dashboard.core.components.dashboard import render_dashboard
from src.dashboard.core.utils.data_cleaning import initialize_session_state
from src.dashboard.core.config.config import load_config
import logging

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Load configuration
config = load_config()

# Initialize session state
initialize_session_state()

# Set page configuration
st.set_page_config(
    page_title="Manga Analytics Dashboard",
    layout="wide",
    page_icon="src/dashboard/core/assets/favicon.png",
    initial_sidebar_state="expanded"
)

# Apply custom CSS
with open("src/dashboard/core/config/styles.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Render sidebar and dashboard
render_sidebar()
render_dashboard()

# Footer
st.markdown("---")
st.markdown(
    """
    <div style="display: flex; justify-content: space-between; align-items: center; color: #6b7280;">
        <p>Data Source: PostgreSQL (manga, chapter) and MongoDB (image_data)</p>
        <p>Updated by update_manga_database_dag</p>
    </div>
    """,
    unsafe_allow_html=True
)
st.sidebar.markdown("---")
st.sidebar.markdown("v1.7.0 | © 2025 Manga Analytics")
