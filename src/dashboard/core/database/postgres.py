import streamlit as st
import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import logging
import time
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


@st.cache_data(ttl=1800, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def load_postgres_data_paginated(table, offset, limit, filters=None):
    """Load paginated data from PostgreSQL with robust error handling."""
    start_time = time.time()
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()

    try:
        query = f"SELECT * FROM {table}"
        params = {}
        if filters:
            conditions = []
            for k, v in filters.items():
                if k == 'published_year':
                    if v['include_null'] and v['year_range']:
                        conditions.append(
                            f"(published_year BETWEEN :year_min AND :year_max OR published_year IS NULL)")
                        params['year_min'] = v['year_range'][0]
                        params['year_max'] = v['year_range'][1]
                    elif v['include_null']:
                        conditions.append("published_year IS NULL")
                    elif v['year_range']:
                        conditions.append(f"published_year BETWEEN :year_min AND :year_max")
                        params['year_min'] = v['year_range'][0]
                        params['year_max'] = v['year_range'][1]
                elif k == 'genres':
                    if v:
                        placeholders = ','.join([f':g{i}' for i in range(len(v))])
                        conditions.append(f"EXISTS (SELECT 1 FROM unnest(genres) g WHERE g IN ({placeholders}))")
                        for i, val in enumerate(v):
                            params[f'g{i}'] = val
                elif isinstance(v, list):
                    placeholders = ','.join([f':p{i}' for i in range(len(v))])
                    conditions.append(f"{k} IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'p{i}'] = val
                else:
                    conditions.append(f"{k} ILIKE :{k}")
                    params[k] = f'%{v}%'
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall())
        query_time = time.time() - start_time
        st.session_state.perf_stats.append({
            'query': f"{table}_paginated",
            'time': query_time,
            'rows': len(df)
        })
        logging.info(f"Loaded {table} data: {len(df)} rows in {query_time:.2f}s")
        return df
    except SQLAlchemyError as e:
        st.error(f"Database error loading {table} data: {str(e)}")
        logging.error(f"Database error loading {table} data: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error loading {table} data: {str(e)}")
        logging.error(f"Unexpected error loading {table} data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_filter_options():
    """Load filter options for status, language, year range, genres, and original language."""
    engine = get_postgres_engine()
    if not engine:
        return [], [], [2000, 2025], [], []

    try:
        with engine.connect() as conn:
            # Status options
            result = conn.execute(text("SELECT DISTINCT status FROM manga ORDER BY status"))
            status_options = [row[0] for row in result]

            # Chapter languages
            result = conn.execute(text("SELECT DISTINCT lang FROM chapter ORDER BY lang"))
            lang_options = [row[0] for row in result]

            # Year range
            result = conn.execute(text("SELECT MIN(published_year), MAX(published_year) FROM manga WHERE published_year IS NOT NULL"))
            year_range = result.fetchone()
            year_min = int(year_range.min) if year_range.min is not None else 2000
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

        return status_options, lang_options, [year_min, year_max], genre_options, language_options

    except Exception as e:
        st.error(f"Error loading filter options: {str(e)}")
        logging.error(f"Error loading filter options: {str(e)}")
        return [], [], [2000, 2025], [], []
