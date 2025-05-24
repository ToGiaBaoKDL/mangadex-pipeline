import streamlit as st
import pandas as pd
from sqlalchemy.sql import text
from src.dashboard.core.database.postgres import get_postgres_engine


@st.cache_data(ttl=36000, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def get_top_genres(filters=None, limit=5):
    """Get top genres by manga count, applying user filters."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        if not isinstance(limit, int) or limit < 1:
            limit = 5  # fallback an toàn

        query = """
        SELECT trim(g) as genre, COUNT(*) as count
        FROM manga, unnest(genres) g
        """
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
                        conditions.append(f"""
                            EXISTS (
                                SELECT 1 FROM unnest(manga.genres) g2
                                WHERE g2 IN ({placeholders})
                            )
                        """)
                        for i, val in enumerate(v):
                            params[f'g{i}'] = val
                elif isinstance(v, list):
                    placeholders = ','.join([f':p{i}' for i in range(len(v))])
                    conditions.append(f"manga.{k} IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'p{i}'] = val
                else:
                    conditions.append(f"manga.{k} ILIKE :{k}")
                    params[k] = f'%{v}%'
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

        query += f" GROUP BY genre ORDER BY count DESC LIMIT {limit}"

        with engine.connect() as conn:
            results = conn.execute(text(query), params)
            df = pd.DataFrame(results.fetchall(), columns=['genre', 'count'])
        return df
    except Exception as e:
        st.error(f"Error fetching top genres: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=36000, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def get_top_languages(filters=None, limit=5):
    """Get top original languages by manga count, applying user filters."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        # Đảm bảo limit hợp lệ
        if not isinstance(limit, int) or limit < 1:
            limit = 5

        query = "SELECT original_language, COUNT(*) as count FROM manga"
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
                        conditions.append(f"""
                            EXISTS (
                                SELECT 1 FROM unnest(genres) g
                                WHERE g IN ({placeholders})
                            )
                        """)
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

        # Gán LIMIT trực tiếp
        query += f" GROUP BY original_language ORDER BY count DESC LIMIT {limit}"

        with engine.connect() as conn:
            results = conn.execute(text(query), params)
            df = pd.DataFrame(results.fetchall(), columns=['original_language', 'count'])
        return df

    except Exception as e:
        st.error(f"Error fetching top languages: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=36000, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def get_genre_language_combinations(filters=None, limit=3):
    """Get top genre-language combinations by manga count."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        if not isinstance(limit, int) or limit < 1:
            limit = 3

        query = """
        SELECT trim(g) as genre, original_language, COUNT(*) as count
        FROM manga, unnest(genres) g
        """
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
                        conditions.append(f"""
                            EXISTS (
                                SELECT 1 FROM unnest(manga.genres) g2
                                WHERE g2 IN ({placeholders})
                            )
                        """)
                        for i, val in enumerate(v):
                            params[f'g{i}'] = val
                elif isinstance(v, list):
                    placeholders = ','.join([f':p{i}' for i in range(len(v))])
                    conditions.append(f"manga.{k} IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'p{i}'] = val
                else:
                    conditions.append(f"manga.{k} ILIKE :{k}")
                    params[k] = f'%{v}%'
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

        query += f" GROUP BY genre, original_language ORDER BY count DESC LIMIT {limit}"

        with engine.connect() as conn:
            results = conn.execute(text(query), params)
            df = pd.DataFrame(results.fetchall(), columns=['genre', 'original_language', 'count'])
        return df

    except Exception as e:
        st.error(f"Error fetching genre-language combinations: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=36000, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def get_filtered_manga_count(filters=None):
    """Get count of manga matching user filters."""
    engine = get_postgres_engine()
    if not engine:
        return 0
    try:
        query = "SELECT COUNT(*) as count FROM manga"
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
        with engine.connect() as conn:
            results = conn.execute(text(query), params)
            df = pd.DataFrame(results.fetchall())
        return int(df['count'].iloc[0]) if not df.empty else 0
    except Exception as e:
        st.error(f"Error fetching filtered manga count: {str(e)}")
        return 0
