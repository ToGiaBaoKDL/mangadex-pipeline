import streamlit as st
import pandas as pd
from sqlalchemy import text
from src.dashboard.core.database.postgres import get_postgres_engine


def sanitize_input(search_term):
    """Sanitize search input to prevent SQL injection."""
    if not isinstance(search_term, str):
        return ""
    # Remove potentially dangerous characters
    return ''.join(c for c in search_term if c.isalnum() or c in [' ', '-', '_']).strip()


def search_manga(search_term, limit=25):
    """Search manga titles using SQL LIKE."""
    search_term = sanitize_input(search_term)
    if not search_term:
        return []
    engine = get_postgres_engine()
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            query = "SELECT title FROM manga WHERE title ILIKE :search_term LIMIT :limit"
            params = {'search_term': f'%{search_term}%', 'limit': limit}
            results = conn.execute(text(query), params)
            titles = pd.DataFrame(results.fetchall(), columns=results.keys())['title'].tolist()
        return titles
    except Exception as e:
        st.error(f"Error in manga search: {str(e)}")
        return []
