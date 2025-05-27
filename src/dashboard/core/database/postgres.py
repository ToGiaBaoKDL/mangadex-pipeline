import streamlit as st
from sqlalchemy.sql import text
import logging
from src.dashboard.core.config.config import pg_config


@st.cache_resource(ttl=3600)
def get_postgres_engine():
    """Get PostgreSQL engine with connection test."""
    try:
        engine = pg_config.engine
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Failed to connect to PostgreSQL: {str(e)}")
        logging.error(f"PostgreSQL connection error: {str(e)}")
        return None


@st.cache_data(ttl=3600)
def load_filter_options():
    """Load filter options for status, year range, genres, and original language."""
    engine = get_postgres_engine()
    if not engine:
        return [], [1900, 2025], [], []

    try:
        with engine.connect() as conn:
            # Status options
            result = conn.execute(text("SELECT DISTINCT status FROM manga ORDER BY status"))
            status_options = [row[0] for row in result]

            # Year range
            result = conn.execute(text("SELECT MIN(published_year), MAX(published_year) FROM manga WHERE published_year IS NOT NULL"))
            year_range = result.fetchone()
            year_min = int(year_range.min) if year_range.min is not None else 1900
            year_max = int(year_range.max) if year_range.max is not None else 2025

            # Genre options from JSON array
            genre_query = """
                SELECT DISTINCT TRIM(g) AS genre
                FROM manga, unnest(genres) AS g
                ORDER BY genre
            """
            result = conn.execute(text(genre_query))
            genre_options = [row[0] for row in result]

            # Original languages
            result = conn.execute(text("SELECT DISTINCT original_language FROM manga ORDER BY original_language"))
            language_options = [row[0] for row in result]

        return status_options, [year_min, year_max], genre_options, language_options

    except Exception as e:
        st.error(f"Error loading filter options: {str(e)}")
        logging.error(f"Error loading filter options: {str(e)}")
        return [], [1900, 2025], [], []
