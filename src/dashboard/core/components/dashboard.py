import streamlit as st
import pandas as pd
from sqlalchemy import text
import re
from src.dashboard.core.database.postgres import (
    get_postgres_engine
)
from src.dashboard.core.utils.export import format_number
from src.dashboard.core.utils.insights import generate_insights
from src.dashboard.core.utils.search import sanitize_input
from src.dashboard.core.components.charts import (
    create_status_pie,
    create_year_vs_mangas_histogram,
    create_genre_bar,
    create_language_treemap,
    create_genre_cooccurrence_heatmap,
    create_chapter_counts_bar
)
from src.dashboard.core.utils.display_image import load_and_display_cover, display_random_cover_images


SAMPLE_ROWS = 100


@st.cache_data(ttl=3600)
def load_quick_stats(selected_manga=None, manga_filters=None):
    """Load quick stats, either global or for a specific manga."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        params = {}
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
            params['title'] = selected_manga
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
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
            # Filtered stats
            conditions = []
            if manga_filters:
                for k, v in manga_filters.items():
                    if k == 'published_year' and v.get('year_range'):
                        if v['include_null']:
                            conditions.append(
                                "(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                        else:
                            conditions.append("m.published_year BETWEEN :year_min AND :year_max")
                        params['year_min'] = v['year_range'][0]
                        params['year_max'] = v['year_range'][1]
                    elif k == 'genres' and v:
                        placeholders = ','.join([f':g{i}' for i in range(len(v))])
                        conditions.append(f"EXISTS (SELECT 1 FROM unnest(m.genres) g WHERE g IN ({placeholders}))")
                        for i, val in enumerate(v):
                            params[f'g{i}'] = val
                    elif k == 'status' and v:
                        placeholders = ','.join([f':s{i}' for i in range(len(v))])
                        conditions.append(f"m.status IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f's{i}'] = val
                    elif k == 'original_language' and v:
                        placeholders = ','.join([f':ol{i}' for i in range(len(v))])
                        conditions.append(f"m.original_language IN ({placeholders})")
                        for i, val in enumerate(v):
                            params[f'ol{i}'] = val

            manga_where = " WHERE " + " AND ".join(conditions) if conditions else ""
            query = f"""
            SELECT 
                COUNT(DISTINCT m.manga_id) as total_manga,
                COUNT(c.chapter_id) as total_chapters,
                COALESCE(SUM(c.pages), 0) as total_images,
                COALESCE(AVG(c.pages), 0) as avg_pages_per_chapter
            FROM manga m
            LEFT JOIN chapter c ON m.manga_id = c.manga_id
            {manga_where}
            """

            # Global stats from summary_metrics
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())

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


@st.cache_data(ttl=3600, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def load_chart_data(filters=None, query_type="aggregate"):
    """Load data for charts with filters applied, fetching all rows."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        params = {}
        conditions = []
        if filters:
            for k, v in filters.items():
                if k == 'published_year' and v.get('year_range'):
                    if v['include_null']:
                        conditions.append(
                            "(m.published_year BETWEEN :year_min AND :year_max OR m.published_year IS NULL)")
                    else:
                        conditions.append("m.published_year BETWEEN :year_min AND :year_max")
                    params['year_min'] = v['year_range'][0]
                    params['year_max'] = v['year_range'][1]
                elif k == 'status' and v:
                    placeholders = ', '.join([f':s{i}' for i in range(len(v))])
                    conditions.append(f"m.status IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f's{i}'] = val
                elif k == 'original_language' and v:
                    placeholders = ', '.join([f':o{i}' for i in range(len(v))])
                    conditions.append(f"m.original_language IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'o{i}'] = val
                elif k == 'title' and v:
                    conditions.append("m.title = :title")
                    params['title'] = v
                elif k == 'genres' and isinstance(v, list) and v:
                    placeholders = ', '.join([f':g{i}' for i in range(len(v))])
                    conditions.append(f"m.genres && ARRAY[{placeholders}]")
                    for i, val in enumerate(v):
                        params[f'g{i}'] = val
                elif isinstance(v, str) and '%' in v:
                    conditions.append(f"m.{k} ILIKE :{k}")
                    params[k] = v
                elif isinstance(v, list) and v:
                    placeholders = ', '.join([f':{k}{i}' for i in range(len(v))])
                    conditions.append(f"m.{k} IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'{k}{i}'] = val
                elif v:
                    conditions.append(f"m.{k} = :{k}")
                    params[k] = v

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        if query_type == "status":
            query = f"""
            SELECT m.status, COUNT(*) as count
            FROM manga AS m
            {where_clause}
            GROUP BY status
            """

            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall(), columns=['status', 'count'])
            return df if not df.empty else pd.DataFrame({'status': ['No Data'], 'count': [0]})
        elif query_type == "genres":
            query = f"""
            SELECT trim(g) as genre, COUNT(*) as count
            FROM manga m, unnest(genres) g
            {where_clause}
            GROUP BY genre
            ORDER BY count DESC
            LIMIT 5
            """
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df if not df.empty else pd.DataFrame({'genre': ['No Data'], 'count': [0]})
        elif query_type == "chapter_trend":
            query = f"""
            SELECT DATE_TRUNC('month', c.created_at)::date as month_year, COUNT(*) as count
            FROM chapter c
            JOIN manga m ON c.manga_id = m.manga_id
            {where_clause}
            GROUP BY month_year
            ORDER BY month_year
            """

            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "year_vs_mangas":
            query = f"""
            SELECT m.published_year, COUNT(m.manga_id) as manga_count, m.title
            FROM manga m
            {where_clause}
            GROUP BY m.published_year, m.title
            """

            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "language":
            query = f"""
            SELECT m.original_language, COUNT(*) as count
            FROM manga AS m
            {where_clause}
            GROUP BY m.original_language
            """

            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "genre_cooccurrence":
            query = f"""
            WITH genres AS (
                SELECT m.manga_id, trim(g) as genre
                FROM manga m, unnest(m.genres) g
            )
            SELECT g1.genre as genre1, g2.genre as genre2, COUNT(*) as count
            FROM genres g1
            JOIN genres g2 ON g1.manga_id = g2.manga_id AND g1.genre < g2.genre
            JOIN manga m ON g1.manga_id = m.manga_id
            {where_clause}
            GROUP BY g1.genre, g2.genre
            ORDER BY count DESC
            LIMIT 100
            """

            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        elif query_type == "chapter_counts":
            query = f"""
            SELECT m.title, COUNT(c.chapter_id) as chapter_count
            FROM manga m
            LEFT JOIN chapter c ON m.manga_id = c.manga_id
            {where_clause}
            GROUP BY m.title
            ORDER BY chapter_count DESC
            LIMIT 5
            """

            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall())
            return df
        else:
            raise ValueError(f"Invalid query_type: {query_type}")
    except Exception as e:
        st.error(f"Error loading chart data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, hash_funcs={dict: lambda x: str(sorted(x.items()))})
def load_manga_df(manga_filters=None, selected_manga=None):
    """Load manga DataFrame with filters applied, limited to 100 rows."""
    engine = get_postgres_engine()
    if not engine:
        return pd.DataFrame()
    try:
        params = {}
        manga_conditions = []
        if selected_manga:
            query = f"""
            SELECT manga_id, title, status, published_year, genres, original_language, updated_at, cover_url
            FROM manga
            WHERE title = :title
            """
            params['title'] = selected_manga
            with engine.connect() as conn:
                results = conn.execute(text(query), params)
                df = pd.DataFrame(results.fetchall(), columns=results.keys())
            if df.empty:
                df = pd.DataFrame(columns=['manga_id', 'title', 'status', 'published_year',
                                           'genres', 'original_language', 'updated_at', 'cover_url'])
            df['manga_id'] = df['manga_id'].astype(str)
            return df

        if manga_filters:

            for k, v in manga_filters.items():
                if k == 'published_year':
                    if v['include_null'] and v['year_range']:
                        manga_conditions.append(f"(published_year BETWEEN :year_min AND :year_max OR published_year IS NULL)")
                        params['year_min'] = v['year_range'][0]
                        params['year_max'] = v['year_range'][1]
                    elif v['include_null']:
                        manga_conditions.append("published_year IS NULL")
                    elif v['year_range']:
                        manga_conditions.append(f"published_year BETWEEN :year_min AND :year_max")
                        params['year_min'] = v['year_range'][0]
                        params['year_max'] = v['year_range'][1]
                elif k == 'genres' and v:
                    placeholders = ','.join([f':g{i}' for i in range(len(v))])
                    manga_conditions.append(f"EXISTS (SELECT 1 FROM unnest(genres) g WHERE g IN ({placeholders}))")
                    for i, val in enumerate(v):
                        params[f'g{i}'] = val
                elif k == 'status' and v:
                    placeholders = ','.join([f':s{i}' for i in range(len(v))])
                    manga_conditions.append(f"status IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f's{i}'] = val
                elif k == 'original_language' and v:
                    placeholders = ','.join([f':ol{i}' for i in range(len(v))])
                    manga_conditions.append(f"original_language IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'ol{i}'] = val
                elif k == 'title' and v:
                    manga_conditions.append("title = :title")
                    params['title'] = v

        manga_where = " WHERE " + " AND ".join(manga_conditions) if manga_conditions else ""
        query = f"""
        SELECT manga_id, title, status, published_year, genres, original_language, updated_at, cover_url
        FROM manga
        {manga_where}
        LIMIT 100
        """
        with engine.connect() as conn:
            results = conn.execute(text(query), params)
            df = pd.DataFrame(results.fetchall(), columns=results.keys())
        if df.empty:
            df = pd.DataFrame(columns=['manga_id', 'title', 'status', 'published_year',
                                       'genres', 'original_language', 'updated_at', 'cover_url'])
        df['manga_id'] = df['manga_id'].astype(str)
        return df
    except Exception as e:
        st.error(f"Error loading manga DataFrame: {str(e)}")
        return pd.DataFrame()


def render_dashboard():
    """Render the main dashboard."""

    def format_insight(text):
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
    if st.session_state.published_year:
        manga_filters['published_year'] = st.session_state.published_year
    if 'selected_manga' in st.session_state and st.session_state.selected_manga:
        manga_filters['title'] = sanitize_input(st.session_state.selected_manga)

    # Load Sample Data for Tables
    with st.spinner("Loading sample manga data..."):
        manga_df = load_manga_df(manga_filters, st.session_state.selected_manga)

    # Dashboard Header
    col_title, col_updated = st.columns([3, 1])
    with col_title:
        st.title("📚 Manga Analytics Dashboard")
    with col_updated:
        st.markdown(f"**Last Refreshed:** {st.session_state.last_refresh} (UTC+7)")

    # Quick Stats
    stats_df = load_quick_stats(st.session_state.selected_manga, manga_filters)

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
        load_and_display_cover(st.session_state.selected_manga)

    # Display random cover images
    if st.session_state.selected_manga is None:
        st.markdown("### 🔥 Random Manga Covers")
        display_random_cover_images(manga_df)

    # Insights
    st.markdown("### 📈 Key Insights")
    with st.expander(" 💡Data Highlights", expanded=True):
        flag, insights = generate_insights(manga_filters, st.session_state.selected_manga)

        if not flag:
            st.warning("No insights available due to data retrieval issues.")
        else:
            for insight_dict in insights:
                insight = f"{insight_dict.get('icon')} {insight_dict.get('tooltip')}: {insight_dict.get('text')}"
                style = format_insight(insight)
                st.markdown(f'<div class="insight-box">{style}</div>', unsafe_allow_html=True)

    # # Export Options
    # if st.session_state.selected_manga is None:
    #     st.markdown("### 📥 Export Data")
    #     export_format = st.selectbox("Select Export Format", ["CSV", "Excel", "Parquet"], key="export_format")
    #     if manga_df is not None and not manga_df.empty:
    #         combined_df = manga_df.merge(chapter_df, on='manga_id', how='left', suffixes=('_manga', '_chapter'))
    #         if export_format in ["CSV", "Excel", "Parquet"]:
    #             st.markdown(export_data(combined_df, "manga_chapter_data", export_format.lower()), unsafe_allow_html=True)

    # Tabs
    tab1, tab2 = st.tabs(["📊 Overview", "📖 Manga Analysis"])
    charts = {}

    with tab1:
        st.header("Overview")
        col1, col2 = st.columns(2)
        if not manga_df.empty and manga_df.shape[0] > 10:
            with col1:
                if not st.session_state.selected_manga:
                    status_data = load_chart_data(query_type="status", filters=manga_filters)
                    fig_status = create_status_pie(status_data)
                    if fig_status:
                        st.plotly_chart(fig_status, use_container_width=True)
                        charts['status_distribution'] = fig_status
                else:
                    st.info("No manga data available for status distribution.")
            with col2:
                if not st.session_state.selected_manga:
                    genre_data = load_chart_data(query_type="genres", filters=manga_filters)
                    fig_genre = create_genre_bar(genre_data)
                    if fig_genre:
                        st.plotly_chart(fig_genre, use_container_width=True)
                        charts['genre_distribution'] = fig_genre
                else:
                    st.info("No genre data available.")

            if not st.session_state.selected_manga:
                language_data = load_chart_data(query_type="language", filters=manga_filters)
                fig_language = create_language_treemap(language_data)
                if fig_language:
                    st.plotly_chart(fig_language, use_container_width=True)
                    charts['language_distribution'] = fig_language
            else:
                st.info("No language data available.")
        else:
            st.warning("Not enough manga data available.")

    with tab2:
        st.header("Manga Analysis")
        col1, col2 = st.columns(2)
        if not manga_df.empty and manga_df.shape[0] > 10:
            with col1:
                if not st.session_state.selected_manga:
                    bar_data = load_chart_data(manga_filters, query_type="chapter_counts")
                    fig_bar = create_chapter_counts_bar(bar_data)
                    if fig_bar:
                        st.plotly_chart(fig_bar, use_container_width=True)
                        charts['bar_distribution'] = fig_bar
                else:
                    st.info("No data for top manga by chapter count")
            with col2:
                if not st.session_state.published_year.get('include_null'):
                    if not st.session_state.selected_manga:
                        scatter_data = load_chart_data(manga_filters, query_type="year_vs_mangas")
                        fig_scatter = create_year_vs_mangas_histogram(scatter_data)
                        if fig_scatter:
                            st.plotly_chart(fig_scatter, use_container_width=True)
                            charts['year_vs_chapters'] = fig_scatter
                    else:
                        st.info("No data for year vs. mangas histogram plot.")
                else:
                    st.warning("Disable 'Include Null Published Year' to plot the published year vs. manga count histogram.")

            if not st.session_state.selected_manga:
                cooccurrence_data = load_chart_data(manga_filters, query_type="genre_cooccurrence")
                fig_cooccurrence = create_genre_cooccurrence_heatmap(cooccurrence_data)
                if fig_cooccurrence:
                    st.plotly_chart(fig_cooccurrence, use_container_width=True)
                    charts['genre_cooccurrence'] = fig_cooccurrence
            else:
                st.info("No genre co-occurrence data available.")
        else:
            st.warning("Not enough manga data available.")

        if not manga_df.empty:
            num_rows = manga_df.shape[0]
            st.markdown(f"""
            <span style="color:#FF7F00; font-size:18px; font-weight:bold; padding:3px;">
                Showing {num_rows} sample mangas
            </span>
            """, unsafe_allow_html=True)
            display_manga = manga_df.copy()
            display_cols = ['title', 'status', 'published_year', 'genres', 'original_language']
            st.write(display_manga[display_cols])
        else:
            st.info("No manga data available.")
