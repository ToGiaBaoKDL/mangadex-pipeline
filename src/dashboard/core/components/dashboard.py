# src/components/dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
from src.dashboard.core.database.postgres import (
    load_postgres_data_paginated,
    get_postgres_engine
)
from src.dashboard.core.utils.export import export_data, format_number
from src.dashboard.core.utils.insights import generate_insights
from src.dashboard.core.utils.search import sanitize_input
from src.dashboard.core.components.charts import (
    create_status_pie,
    create_year_vs_chapters_scatter,
    create_genre_bar,
    create_language_treemap,
    create_genre_cooccurrence_heatmap,
    create_avg_pages_bar,
    create_chapter_counts_bar
)
from src.dashboard.core.utils.statistics import (
    get_filtered_manga_count,
    get_genre_language_combinations
)
from sqlalchemy import text
import re


SAMPLE_ROWS = 100


@st.cache_data(ttl=3600)
def load_quick_stats(selected_manga=None):
    """Load quick stats, either global or for a specific manga."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        if selected_manga:
            # Stats for specific manga
            query = """
            SELECT 
                1 as total_manga,
                COUNT(c.chapter_id) as total_chapters,
                COALESCE(SUM(c.pages), 0) as total_images,
                COALESCE(AVG(c.pages), 0) as avg_pages_per_chapter
            FROM manga m
            LEFT JOIN chapter c ON m.manga_id = c.manga_id
            WHERE m.title = :title
            GROUP BY m.manga_id
            """
            with engine.connect() as conn:
                results = conn.execute(text(query), {'title': selected_manga})
                df = pd.DataFrame(results.fetchall(), columns=results.keys())

            # Ensure all metrics are present
            if df.empty:
                df = pd.DataFrame([{
                    'total_manga': 0,
                    'total_chapters': 0,
                    'total_images': 0,
                    'avg_pages_per_chapter': 0
                }])
        else:
            # Global stats from summary_metrics
            with engine.connect() as conn:
                results = conn.execute(text("SELECT * FROM summary_metrics"))
                df = pd.DataFrame(results.fetchall(), columns=results.keys())

            df = df.pivot_table(index=None, columns="metric_name", values="metric_value", aggfunc="first")
            if df.empty:
                df = pd.DataFrame([{
                    'total_manga': 0,
                    'total_chapters': 0,
                    'total_images': 0,
                    'avg_pages_per_chapter': 0
                }])
        return df
    except Exception as e:
        st.error(f"Error loading quick stats: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=36000, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def load_chart_data(filters=None, query_type="aggregate"):
    """Load data for charts with filters applied, fetching all rows."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        if query_type == "status":
            query = "SELECT status, COUNT(*) as count FROM manga"
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
            query += " GROUP BY status"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "genres":
            query = "SELECT trim(g) as genre, COUNT(*) as count FROM manga, unnest(genres) g"
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if k == 'published_year':
                        if v['include_null'] and v['year_range']:
                            conditions.append(
                                f"(manga.published_year BETWEEN :year_min AND :year_max OR manga.published_year IS NULL)")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                        elif v['include_null']:
                            conditions.append("manga.published_year IS NULL")
                        elif v['year_range']:
                            conditions.append(f"manga.published_year BETWEEN :year_min AND :year_max")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                    elif k == 'genres':
                        if v:
                            placeholders = ','.join([f':g{i}' for i in range(len(v))])
                            conditions.append(f"EXISTS (SELECT 1 FROM unnest(manga.genres) g2 WHERE g2 IN ({placeholders}))")
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
            query += " GROUP BY genre ORDER BY count DESC LIMIT 5"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "chapter_trend":
            query = "SELECT DATE_TRUNC('month', created_at)::date as month_year, COUNT(*) as count FROM chapter"
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if isinstance(v, list):
                        placeholders = ','.join([f':p{i}' for i in range(len(v))])
                        conditions.append(f"{k} IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'p{i}'] = val
                    else:
                        conditions.append(f"{k} ILIKE :{k}")
                        params[k] = f'%{v}%'
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            query += " GROUP BY month_year ORDER BY month_year"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "year_vs_chapters":
            query = """
            SELECT m.published_year, COUNT(c.chapter_id) as chapter_count, m.title
            FROM manga m
            LEFT JOIN chapter c ON m.manga_id = c.manga_id
            """
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if k == 'published_year':
                        if v['include_null'] and v['year_range']:
                            conditions.append(
                                f"(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                        elif v['include_null']:
                            conditions.append("m.published_year IS NULL")
                        elif v['year_range']:
                            conditions.append(f"m.published_year BETWEEN :year_min AND :year_max")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                    elif k == 'genres':
                        if v:
                            placeholders = ','.join([f':g{i}' for i in range(len(v))])
                            conditions.append(f"EXISTS (SELECT 1 FROM unnest(m.genres) g WHERE g IN ({placeholders}))")
                            for i, val in enumerate(v):
                                params[f'g{i}'] = val
                    elif isinstance(v, list):
                        placeholders = ','.join([f':p{i}' for i in range(len(v))])
                        conditions.append(f"m.{k} IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'p{i}'] = val
                    else:
                        conditions.append(f"m.{k} ILIKE :{k}")
                        params[k] = f'%{v}%'
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            query += " GROUP BY m.published_year, m.title"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "language":
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
            query += " GROUP BY original_language"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "genre_cooccurrence":
            query = """
            WITH genres AS (
                SELECT m.manga_id, trim(g1) as genre
                FROM manga m, unnest(m.genres) g1
            )
            SELECT g1.genre as genre1, g2.genre as genre2, COUNT(*) as count
            FROM genres g1
            JOIN genres g2 ON g1.manga_id = g2.manga_id AND g1.genre < g2.genre
            JOIN manga m ON g1.manga_id = m.manga_id
            """
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if k == 'published_year':
                        if v['include_null'] and v['year_range']:
                            conditions.append(
                                f"(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                        elif v['include_null']:
                            conditions.append("m.published_year IS NULL")
                        elif v['year_range']:
                            conditions.append(f"m.published_year BETWEEN :year_min AND :year_max")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                    elif k == 'genres':
                        if v:
                            placeholders = ','.join([f':g{i}' for i in range(len(v))])
                            conditions.append(f"EXISTS (SELECT 1 FROM unnest(m.genres) g WHERE g IN ({placeholders}))")
                            for i, val in enumerate(v):
                                params[f'g{i}'] = val
                    elif isinstance(v, list):
                        placeholders = ','.join([f':p{i}' for i in range(len(v))])
                        conditions.append(f"m.{k} IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'p{i}'] = val
                    else:
                        conditions.append(f"m.{k} ILIKE :{k}")
                        params[k] = f'%{v}%'
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            query += " GROUP BY g1.genre, g2.genre ORDER BY count DESC LIMIT 100"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "chapter_pages":
            query = """
                    SELECT c.pages
                    FROM chapter c
                    JOIN manga m ON c.manga_id = m.manga_id
                    WHERE c.pages IS NOT NULL
                    """
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if k == 'lang' and v:
                        if isinstance(v, list):
                            placeholders = ','.join([f':p{i}' for i in range(len(v))])
                            conditions.append(f"c.{k} IN ({placeholders})")
                            for i, val in enumerate(v):
                                params[f'p{i}'] = val
                        else:
                            conditions.append(f"c.{k} ILIKE :{k}")
                            params[k] = f'%{v}%'
                    elif k == 'published_year':
                        if v['include_null'] and v['year_range']:
                            conditions.append(f"(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                        elif v['include_null']:
                            conditions.append("m.published_year IS NULL")
                        elif v['year_range']:
                            conditions.append(f"m.published_year BETWEEN :year_min AND :year_max")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                    elif k == 'genres':
                        if v:
                            placeholders = ','.join([f':g{i}' for i in range(len(v))])
                            conditions.append(
                                f"EXISTS (SELECT 1 FROM unnest(m.genres) g WHERE g IN ({placeholders}))")
                            for i, val in enumerate(v):
                                params[f'g{i}'] = val
                    elif isinstance(v, list):
                        placeholders = ','.join([f':p{i}' for i in range(len(v))])
                        conditions.append(f"m.{k} IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'p{i}'] = val
                    else:
                        conditions.append(f"m.{k} ILIKE :{k}")
                        params[k] = f'%{v}%'
                if conditions:
                    query += " AND " + " AND ".join(conditions)
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "avg_pages":
            query = """
                    SELECT m.title, AVG(c.pages) as avg_pages
                    FROM manga m
                    JOIN chapter c ON m.manga_id = c.manga_id
                    WHERE c.pages IS NOT NULL
                    """
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if k == 'lang' and v:
                        if isinstance(v, list):
                            placeholders = ','.join([f':p{i}' for i in range(len(v))])
                            conditions.append(f"c.{k} IN ({placeholders})")
                            for i, val in enumerate(v):
                                params[f'p{i}'] = val
                        else:
                            conditions.append(f"c.{k} ILIKE :{k}")
                            params[k] = f'%{v}%'
                    elif k == 'published_year':
                        if v['include_null'] and v['year_range']:
                            conditions.append(
                                f"(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                        elif v['include_null']:
                            conditions.append("m.published_year IS NULL")
                        elif v['year_range']:
                            conditions.append(f"m.published_year BETWEEN :year_min AND :year_max")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                    elif k == 'genres':
                        if v:
                            placeholders = ','.join([f':g{i}' for i in range(len(v))])
                            conditions.append(
                                f"EXISTS (SELECT 1 FROM unnest(m.genres) g WHERE g IN ({placeholders}))")
                            for i, val in enumerate(v):
                                params[f'g{i}'] = val
                    elif isinstance(v, list):
                        placeholders = ','.join([f':p{i}' for i in range(len(v))])
                        conditions.append(f"m.{k} IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'p{i}'] = val
                    else:
                        conditions.append(f"m.{k} ILIKE :{k}")
                        params[k] = f'%{v}%'
                if conditions:
                    query += " AND " + " AND ".join(conditions)
            query += " GROUP BY m.title ORDER BY COUNT(c.chapter_id) DESC LIMIT 5"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "chapter_counts":
            query = """
                    SELECT m.title, COUNT(c.chapter_id) as chapter_count
                    FROM manga m
                    LEFT JOIN chapter c ON m.manga_id = c.manga_id
                    """
            params = {}
            if filters:
                conditions = []
                for k, v in filters.items():
                    if k == 'lang' and v:
                        if isinstance(v, list):
                            placeholders = ','.join([f':p{i}' for i in range(len(v))])
                            conditions.append(f"c.{k} IN ({placeholders})")
                            for i, val in enumerate(v):
                                params[f'p{i}'] = val
                        else:
                            conditions.append(f"c.{k} ILIKE :{k}")
                            params[k] = f'%{v}%'
                    elif k == 'published_year':
                        if v['include_null'] and v['year_range']:
                            conditions.append(
                                f"(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                        elif v['include_null']:
                            conditions.append("m.published_year IS NULL")
                        elif v['year_range']:
                            conditions.append(f"m.published_year BETWEEN :year_min AND :year_max")
                            params['year_min'] = v['year_range'][0]
                            params['year_max'] = v['year_range'][1]
                    elif k == 'genres':
                        if v:
                            placeholders = ','.join([f':g{i}' for i in range(len(v))])
                            conditions.append(
                                f"EXISTS (SELECT 1 FROM unnest(m.genres) g WHERE g IN ({placeholders}))")
                            for i, val in enumerate(v):
                                params[f'g{i}'] = val
                    elif isinstance(v, list):
                        placeholders = ','.join([f':p{i}' for i in range(len(v))])
                        conditions.append(f"m.{k} IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'p{i}'] = val
                    else:
                        conditions.append(f"m.{k} ILIKE :{k}")
                        params[k] = f'%{v}%'
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            query += " GROUP BY m.title ORDER BY chapter_count DESC LIMIT 5"
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        else:
            raise ValueError(f"Invalid query_type: {query_type}")
    except Exception as e:
        st.error(f"Error loading chart data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_cover_url(selected_manga=None):
    """Load cover URL for the selected manga."""
    if not selected_manga:
        return None
    engine = get_postgres_engine()
    if not engine:
        return None
    try:
        query = """
        SELECT cover_url
        FROM manga
        WHERE title = :title
        """
        with engine.connect() as conn:
            results = conn.execute(text(query), {'title': selected_manga})
            df = pd.DataFrame(results.fetchall())
        if not df.empty:
            return df['cover_url'].iloc[0]
        return None
    except Exception as e:
        st.error(f"Error loading cover URL: {str(e)}")
        return None


def render_dashboard():
    """Render the main dashboard."""

    def format_insight(text):
        text = text.replace("**", "")
        return re.sub(
            r"^(.*?):",
            r'<span style="color:orange; font-weight:bold;">\1:</span>',
            text
        )

    # Apply Filters
    manga_filters = {}
    if st.session_state.status_filter:
        manga_filters['status'] = st.session_state.status_filter
    if st.session_state.genres_filter:
        manga_filters['genres'] = st.session_state.genres_filter
    if st.session_state.original_language_filter:
        manga_filters['original_language'] = st.session_state.original_language_filter
    if st.session_state.year_filter:
        manga_filters['published_year'] = st.session_state.year_filter
    if 'selected_manga' in st.session_state and st.session_state.selected_manga:
        manga_filters['title'] = sanitize_input(st.session_state.selected_manga)
    chapter_filters = {}
    if st.session_state.lang_filter:
        chapter_filters['lang'] = st.session_state.lang_filter

    # Load Sample Data for Tables
    with st.spinner("Loading sample manga data..."):
        manga_df = load_postgres_data_paginated("manga", 0, SAMPLE_ROWS, manga_filters)
    with st.spinner("Loading sample chapter data..."):
        chapter_df = load_postgres_data_paginated("chapter", 0, SAMPLE_ROWS, chapter_filters)
    filtered_manga = manga_df
    if chapter_filters:
        engine = get_postgres_engine()
        if engine:
            query = "SELECT DISTINCT manga_id FROM chapter WHERE " + " AND ".join([f"{k} = :{k}" for k in chapter_filters.keys()])
            with engine.connect() as conn:
                results = conn.execute(text(query), chapter_filters)
                filtered_manga_ids = pd.DataFrame(results.fetchall())['manga_id'].tolist()
            filtered_manga = manga_df[manga_df['manga_id'].isin(filtered_manga_ids)] if not manga_df.empty else manga_df

    # Dashboard Header
    col_title, col_updated = st.columns([3, 1])
    with col_title:
        st.title("📚 Manga Analytics Dashboard")
    with col_updated:
        st.markdown(f"**Last Refreshed:** {st.session_state.last_refresh} (UTC+7)")

    # Quick Stats
    stats_df = load_quick_stats(st.session_state.selected_manga)

    if not stats_df.empty:
        total_manga = int(stats_df['total_manga'].iloc[0])
        total_chapters = int(stats_df['total_chapters'].iloc[0])
        total_images = int(stats_df['total_images'].iloc[0])
        avg_pages = float(round(stats_df['avg_pages_per_chapter'].iloc[0], 2))
    else:
        st.info("No quick stats available.")
        return

    with st.expander("📊 Quick Stats", expanded=True):
        cols = st.columns(4)
        with cols[0]:
            st.metric("Total Manga", format_number(total_manga))
        with cols[1]:
            st.metric("Total Chapters", format_number(total_chapters))
        with cols[2]:
            st.metric("Total Images", format_number(total_images))
        with cols[3]:
            st.metric("Avg Pages/Chapter", avg_pages)

    # Load cover image
    if st.session_state.selected_manga is not None:
        cover_url = load_cover_url(st.session_state.get("selected_manga"))

        if cover_url:
            try:
                st.image(cover_url, caption=f"{st.session_state.get('selected_manga')}", use_container_width=True)
            except Exception as e:
                st.warning("⚠️ Failed to load image from the provided URL.")
                st.text(f"Details: {str(e)}")
        else:
            st.info("ℹ️ No cover image available for this manga.")

    # Insights
    st.markdown("### 📈 Key Insights")
    with st.expander(" 💡Data Highlights", expanded=True):
        insights = generate_insights()
        insights.append(
            f"**🔍 Filtered Manga**: {format_number(get_filtered_manga_count(manga_filters))} manga match your filters.")
        genre_lang = get_genre_language_combinations(manga_filters, limit=1)
        if not genre_lang.empty and not st.session_state.selected_manga:
            top_combo = f"{genre_lang.iloc[0]['genre']} in {genre_lang.iloc[0]['original_language']}"
            insights.append(f"**🌐 Top Genre-Language Combo**: {top_combo} is the most common combination.")
        for insight in insights:
            style = format_insight(insight)
            st.markdown(f'<div class="insight-box">{style}</div>', unsafe_allow_html=True)

    # Export Options
    if st.session_state.selected_manga is None:
        st.markdown("### 📥 Export Data")
        export_format = st.selectbox("Select Export Format", ["CSV", "Excel", "Parquet"], key="export_format")
        if manga_df is not None and not manga_df.empty:
            combined_df = manga_df.merge(chapter_df, on='manga_id', how='left', suffixes=('_manga', '_chapter'))
            if export_format in ["CSV", "Excel", "Parquet"]:
                st.markdown(export_data(combined_df, "manga_chapter_data", export_format.lower()), unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "📖 Manga Analysis", "📝 Chapter Analysis"])
    charts = {}

    with tab1:
        st.header("Overview")
        col1, col2 = st.columns(2)
        with col1:
            status_data = load_chart_data(query_type="status", filters=manga_filters)
            fig_status = create_status_pie(status_data)
            if fig_status:
                st.plotly_chart(fig_status, use_container_width=True)
                charts['status_distribution'] = fig_status
            else:
                st.info("No manga data available for status distribution.")
        with col2:
            genre_data = load_chart_data(query_type="genres", filters=manga_filters)
            fig_genre = create_genre_bar(genre_data)
            if fig_genre:
                st.plotly_chart(fig_genre, use_container_width=True)
                charts['genre_distribution'] = fig_genre
            else:
                st.info("No genre data available.")
        col1, col2 = st.columns(2)
        with col1:
            language_data = load_chart_data(query_type="language", filters=manga_filters)
            fig_language = create_language_treemap(language_data)
            if fig_language:
                st.plotly_chart(fig_language, use_container_width=True)
                charts['language_distribution'] = fig_language
            else:
                st.info("No language data available.")
        with col2:
            trend_data = load_chart_data(query_type="chapter_trend", filters=chapter_filters)
            valid_manga = manga_df is not None and not manga_df.empty

            if not trend_data.empty and valid_manga:
                trend_data['month_year'] = pd.to_datetime(trend_data['month_year']).dt.strftime('%Y-%m')
                fig_trend = px.line(
                    trend_data,
                    x='month_year',
                    y='count',
                    title="📅 Chapter Publications Over Time",
                    markers=True,
                    line_shape='spline',
                    color_discrete_sequence=['#2563eb']
                )
                fig_trend.update_layout(
                    title_font=dict(size=18, color='#FF7F00'),
                    title_x=0.0
                )
                st.plotly_chart(fig_trend, use_container_width=True)
                charts['chapter_trend'] = fig_trend
            else:
                st.info("No chapter data available for trend.")

    with tab2:
        st.header("Manga Analysis")
        col1, col2 = st.columns(2)
        with col1:
            if not filtered_manga.empty:
                query = """
                SELECT DATE_TRUNC('month', updated_at)::date as month_year, COUNT(*) as count
                FROM manga
                WHERE updated_at IS NOT NULL
                """
                params = {}
                if manga_filters:
                    conditions = []
                    for k, v in manga_filters.items():
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
                        query += " AND " + " AND ".join(conditions)
                query += " GROUP BY month_year ORDER BY month_year"
                engine = get_postgres_engine()
                with engine.connect() as conn:
                    results = conn.execute(text(query), params)
                    updates_by_month = pd.DataFrame(results.fetchall())
                updates_by_month['month_year'] = pd.to_datetime(updates_by_month['month_year'], errors='coerce')
                updates_by_month = updates_by_month.dropna(subset=['month_year'])
                updates_by_month['month_year'] = updates_by_month['month_year'].dt.strftime('%Y-%m')
                fig_updates = px.line(
                    updates_by_month,
                    x='month_year',
                    y='count',
                    title="📅 Manga Updates Over Time",
                    markers=True,
                    line_shape='spline',
                    color_discrete_sequence=['#2563eb']
                )
                fig_updates.update_layout(
                    title_font=dict(size=18, color='#FF7F00'),
                    title_x=0.0
                )
                st.plotly_chart(fig_updates, use_container_width=True)
                charts['manga_updates'] = fig_updates
            else:
                st.info("No manga data available.")
        with col2:
            scatter_data = load_chart_data(manga_filters, query_type="year_vs_chapters")
            fig_scatter = create_year_vs_chapters_scatter(scatter_data)
            if fig_scatter:
                st.plotly_chart(fig_scatter, use_container_width=True)
                charts['year_vs_chapters'] = fig_scatter
            else:
                st.info("No data for year vs. chapters scatter plot.")
        cooccurrence_data = load_chart_data(manga_filters, query_type="genre_cooccurrence")
        fig_cooccurrence = create_genre_cooccurrence_heatmap(cooccurrence_data)
        if fig_cooccurrence:
            st.plotly_chart(fig_cooccurrence, use_container_width=True)
            charts['genre_cooccurrence'] = fig_cooccurrence
        else:
            st.info("No genre co-occurrence data available.")
        if not manga_df.empty:
            st.markdown(f"""
            <span style="color:#FF7F00; font-size:18px; font-weight:bold;">
                Showing {SAMPLE_ROWS} sample mangas
            </span>
            """, unsafe_allow_html=True)
            display_manga = manga_df.copy()
            display_manga['updated_at'] = display_manga['updated_at'].dt.strftime('%Y-%m-%d')
            display_cols = ['manga_id', 'title', 'status', 'published_year', 'updated_at', 'genres', 'original_language']
            st.dataframe(
                display_manga[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "title": st.column_config.TextColumn("Title", help="Click to sort or filter"),
                    "status": st.column_config.SelectboxColumn("Status", options=st.session_state.status_filter),
                    "published_year": st.column_config.NumberColumn("Year"),
                    "updated_at": st.column_config.DatetimeColumn("Updated", format="YYYY-MM-DD"),
                    "genres": st.column_config.TextColumn("Genres"),
                    "original_language": st.column_config.TextColumn("Original Language")
                }
            )
        else:
            st.info("No manga data available.")

    with tab3:
        st.header("Chapter Analysis")
        col1, col2 = st.columns(2)
        with col1:
            avg_pages_data = load_chart_data(query_type="avg_pages", filters={**manga_filters, **chapter_filters})
            fig_avg_pages = create_avg_pages_bar(avg_pages_data)
            if fig_avg_pages:
                st.plotly_chart(fig_avg_pages, use_container_width=True)
                charts['avg_pages'] = fig_avg_pages
            else:
                st.info("No average pages data available.")
        with col2:
            chapter_counts_data = load_chart_data(query_type="chapter_counts",
                                                  filters={**manga_filters, **chapter_filters})
            fig_chapter_counts = create_chapter_counts_bar(chapter_counts_data)
            if fig_chapter_counts:
                st.plotly_chart(fig_chapter_counts, use_container_width=True)
                charts['chapter_counts'] = fig_chapter_counts
            else:
                st.info("No chapter data available for manga.")

        valid_manga = manga_df is not None and not manga_df.empty
        valid_chapter = chapter_df is not None and not chapter_df.empty

        if valid_manga and valid_chapter:
            st.markdown(f"""
            <span style="color:#FF7F00; font-size:18px; font-weight:bold;">
                Showing {SAMPLE_ROWS} sample chapters
            </span>
            """, unsafe_allow_html=True)

            display_chapter = chapter_df.merge(manga_df[['manga_id']], on='manga_id', how='left')
            display_chapter['created_at'] = pd.to_datetime(display_chapter['created_at']).dt.strftime('%Y-%m-%d')
            display_cols = ['manga_id', 'chapter_id', 'chapter_number', 'title', 'lang', 'pages', 'created_at']
            st.dataframe(
                display_chapter[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "title": st.column_config.TextColumn("Chapter Title"),
                    "lang": st.column_config.SelectboxColumn("Language", options=st.session_state.lang_filter),
                    "pages": st.column_config.NumberColumn("Pages"),
                    "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD")
                }
            )
        else:
            st.info("No chapter data available.")
