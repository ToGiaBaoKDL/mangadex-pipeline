import streamlit as st
import pandas as pd
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
import requests
from sqlalchemy import text
import re
import json
import base64


SAMPLE_ROWS = 100


def fetch_cover_image(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mangadex.org/"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.content
        else:
            st.warning(f"Cannot parse image: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"Error when fetching image: {e}")
        return None


def image_to_base64(image_bytes):
    return base64.b64encode(image_bytes).decode("utf-8")


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


@st.cache_data(ttl=3600, show_spinner=False)
def load_and_display_cover(selected_manga=None):
    """Load and display cover image for the selected manga with enhanced styling and tooltip."""
    engine = get_postgres_engine()
    if not selected_manga or not engine:
        return None

    try:
        # Enhanced query to get more manga details for tooltip
        query = """
        SELECT title, cover_url, status, genres, published_year
        FROM manga
        WHERE title = :title
        """
        with engine.connect() as conn:
            results = conn.execute(text(query), {'title': selected_manga})
            df = pd.DataFrame(results.fetchall())

        if df.empty or df['cover_url'].iloc[0] is None or df['cover_url'].iloc[0] == '':
            st.info("📚 No cover image available for the selected manga.")
            return None

        manga_data = df.iloc[0].to_dict()

        # Enhanced CSS for single cover display with tooltip
        st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');

        .single-cover-item {
            position: relative;
            text-align: center;
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            transform-origin: center;
            display: inline-block;
            margin: 20px auto; /* Change this line */
            /* Add these new lines */
            width: 100%;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }

        .single-cover-item img {
            width: 200px;
            height: 300px;
            object-fit: cover;
            border-radius: 15px;
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 6px 25px rgba(0, 0, 0, 0.2);
            border: 4px solid transparent;
            background: linear-gradient(white, white) padding-box,
                        linear-gradient(45deg, #667eea, #764ba2) border-box;
        }

        .single-cover-item img:hover {
            transform: translateY(-10px) scale(1.08);
            box-shadow: 0 15px 50px rgba(0, 0, 0, 0.3);
            border-color: #667eea;
        }

        .single-cover-tooltip {
            visibility: hidden;
            width: 320px;
            background: linear-gradient(135deg, #2c3e50, #34495e);
            color: #fff;
            text-align: justify;
            border-radius: 15px;
            padding: 10px;
            position: absolute;
            z-index: 1500;
            top: -20px;
            left: 50%;
            transform: translateX(-50%) translateY(-100%);
            opacity: 0;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            font-size: 14px;
            font-family: 'Inter', sans-serif;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.4);
            backdrop-filter: blur(15px);
            line-height: 1.5;
        }
        
        .single-cover-tooltip::after {
            content: "";
            position: absolute;
            top: 100%;
            left: 50%;
            margin-left: -10px;
            border-width: 10px;
            border-style: solid;
            border-color: #2c3e50 transparent transparent transparent;
        }
        
        .single-cover-item:hover .single-cover-tooltip {
            visibility: visible;
            opacity: 1;
            transform: translateX(-50%) translateY(-100%) translateY(-10px);
        }
        .single-cover-caption {
            font-size: 16px;
            font-weight: 600;
            margin-top: 15px;
            max-width: 200px;
            color: #2c3e50;
            font-family: 'Inter', sans-serif;
            transition: color 0.3s ease;
            line-height: 1.3;
        }

        .single-cover-item:hover .single-cover-caption {
            color: #667eea;
        }

        .tooltip-section {
            display: flex;
            align-items: flex-start;
            gap: 5px; /* Reduced gap for tighter spacing */
            margin-bottom: 8px;
        }
    
        .tooltip-label {
            font-weight: 600;
            flex-shrink: 0;
            display: inline; /* Ensure inline behavior within flex */
            white-space: nowrap; /* Prevent unintended wrapping */
        }
    
        .tooltip-value {
            color: #e8f4fd;
            flex-grow: 1;
            display: inline; /* Ensure inline behavior within flex */
            white-space: normal; /* Allow wrapping for long values */
        }

        .status-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
            text-transform: uppercase;
        }

        .status-ongoing {
            background-color: #48bb78;
            color: white;
        }

        .status-completed {
            background-color: #4299e1;
            color: white;
        }

        .status-hiatus {
            background-color: #ed8936;
            color: white;
        }

        .status-cancelled {
            background-color: #f56565;
            color: white;
        }
        
            .tooltip-label.title {
                color: #64b5f6;
            }
        
            .tooltip-label.status {
                color: #81c784;
            }
        
            .tooltip-label.genres {
                color: #ffb74d;
            }
        
            .tooltip-label.published {
                color: #f48fb1;
            }

        @media (max-width: 768px) {
            .single-cover-item img {
                width: 160px;
                height: 240px;
            }
            .single-cover-tooltip {
                width: 240px;
                right: auto;
                left: 50%;
                top: -20px;
                transform: translateX(-50%);
                font-size: 13px;
                padding: 15px;
            }
            .single-cover-tooltip::after {
                top: 100%;
                right: auto;
                left: 50%;
                margin-left: -10px;
                margin-top: 0;
                border-color: #2c3e50 transparent transparent transparent;
            }
            .single-cover-item:hover .single-cover-tooltip {
                transform: translateX(-50%) translateY(-10px);
            }
            .single-cover-caption {
                font-size: 14px;
            }
        }
        </style>
        """, unsafe_allow_html=True)

        # Parse genres for tooltip
        try:
            if isinstance(manga_data.get('genres'), str):
                genres = [g['value'] for g in json.loads(manga_data['genres'])] if manga_data.get('genres') else []
            else:
                genres = manga_data.get('genres', []) if manga_data.get('genres') else []
            genres_str = ", ".join(genres[:3]) if genres else "Unknown"  # Limit to 3 genres for cleaner display
            if len(genres) > 3:
                genres_str += f" +{len(genres) - 3} more"
        except Exception:
            genres_str = "Unknown"

        # Format status with badge
        status = manga_data.get('status', 'Unknown').lower()
        status_class = f"status-{status.replace(' ', '-')}"
        if status not in ['ongoing', 'completed', 'hiatus', 'cancelled']:
            status_class = "status-ongoing"  # Default styling

        # Create enhanced tooltip content
        tooltip_content = f"""
        <div class="tooltip-section">
            <span class="tooltip-label title">Title:</span>
            <span class="tooltip-value">{manga_data['title']}</span>
        </div>
        <div class="tooltip-section">
            <span class="tooltip-label status">Status:</span>
            <span class="status-badge {status_class}">{manga_data.get('status', 'Unknown')}</span>
        </div>
        <div class="tooltip-section">
            <span class="tooltip-label genres">Genres:</span>
            <span class="tooltip-value">{genres_str}</span>
        </div>
        <div class="tooltip-section">
            <span class="tooltip-label published">Published:</span>
            <span class="tooltip-value">{manga_data.get('published_year', 'Unknown')}</span>
        </div>
        """

        # Display the enhanced cover
        image_bytes = fetch_cover_image(manga_data["cover_url"])
        if image_bytes:
            img_base64 = image_to_base64(image_bytes)
            cover_html = f"""
            <div class="single-cover-item">
                <img src="data:image/jpeg;base64,{img_base64}" alt="{manga_data['title']}" loading="lazy">
                <div class="single-cover-tooltip">{tooltip_content}</div>
                <div class="single-cover-caption">{manga_data['title']}</div>
            </div>
            """

            st.markdown(cover_html, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"❌ Error loading cover: {str(e)}")
        return None


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
                if k == 'year_filter':
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


def display_random_cover_images(manga_df):
    """Display a carousel of 3 random manga cover images from a selection of 9,
    with enhanced navigation arrows, smooth animations, and hover tooltips."""

    # Filter manga with valid cover URLs
    manga_df['genres'] = manga_df['genres'].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    valid_manga = manga_df[manga_df['cover_url'].notna() & (manga_df['cover_url'] != '')][
        ['title', 'cover_url', 'status', 'genres', 'published_year']]

    if valid_manga.empty:
        st.info("No cover images available for the selected filters.")
        return

    # Initialize session state for carousel
    if 'selected_covers' not in st.session_state or st.session_state.get('manga_filters_changed'):
        # Select 10 random manga
        num_to_select = min(9, len(valid_manga))
        st.session_state['selected_covers'] = valid_manga.sample(n=num_to_select, random_state=None).to_dict('records')
        st.session_state['cover_index'] = 0
        st.session_state['manga_filters_changed'] = False

    covers = st.session_state['selected_covers']
    index = st.session_state['cover_index']
    max_index = max(0, len(covers) - 3)  # Ensure at least 3 covers are shown if possible

    # Enhanced CSS for carousel with prettier buttons and smooth animations
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');

    .cover-carousel {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 25px;
        transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .cover-item {
        position: relative;
        text-align: center;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        transform-origin: center;
    }

    .cover-item img {
        width: 160px;
        height: 240px;
        object-fit: cover;
        border-radius: 12px;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
        border: 3px solid transparent;
        background: linear-gradient(white, white) padding-box,
                    linear-gradient(45deg, #667eea, #764ba2) border-box;
    }

    .cover-item img:hover {
        transform: translateY(-8px) scale(1.05);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.25);
        border-color: #667eea;
    }

    .cover-tooltip {
        visibility: hidden;
        width: 250px;
        background: linear-gradient(135deg, #2c3e50, #34495e);
        color: #fff;
        text-align: left;
        border-radius: 12px;
        padding: 15px;
        position: absolute;
        z-index: 1000;
        bottom: 110%;
        left: 50%;
        margin-left: -110px;
        opacity: 0;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.5, 1);
        font-size: 13px;
        font-family: 'Inter', sans-serif;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(10px);
    }

    .cover-tooltip::after {
        content: "";
        position: absolute;
        top: 100%;
        left: 50%;
        margin-left: -8px;
        border-width: 8px;
        border-style: solid;
        border-color: #2c3e50 transparent transparent transparent;
    }

    .cover-item:hover .cover-tooltip {
        visibility: visible;
        opacity: 1;
        transform: translateY(-5px);
    }

    .carousel-nav {
        width: 56px;
        height: 56px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
        cursor: pointer;
        user-select: none;
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        border-radius: 50%;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 4px 20px rgba(102, 126, 234, 0.3);
        border: none;
        font-weight: 600;
        position: relative;
        overflow: hidden;
    }

    .carousel-nav::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
        transition: left 0.5s;
    }

    .carousel-nav:hover {
        transform: translateY(-2px) scale(1.05);
        box-shadow: 0 8px 30px rgba(102, 126, 234, 0.4);
        background: linear-gradient(135deg, #5a67d8, #6b46c1);
    }

    .carousel-nav:hover::before {
        left: 100%;
    }

    .carousel-nav:active {
        transform: translateY(0) scale(0.95);
    }

    .carousel-nav.disabled {
        cursor: not-allowed;
        opacity: 0.4;
        background: linear-gradient(135deg, #a0a0a0, #888888);
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        transform: none;
    }

    .carousel-nav.disabled:hover {
        transform: none;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    }

    .carousel-nav.disabled::before {
        display: none;
    }

    .cover-caption {
        font-size: 13px;
        font-weight: 500;
        margin-top: 12px;
        max-width: 160px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        color: #2c3e50;
        font-family: 'Inter', sans-serif;
        transition: color 0.3s ease;
    }

    .cover-item:hover .cover-caption {
        color: #667eea;
    }

    .carousel-indicators {
        display: flex;
        justify-content: center;
        gap: 8px;
        margin-top: 20px;
    }

    .indicator-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #cbd5e0;
        transition: all 0.3s ease;
        cursor: pointer;
    }

    .indicator-dot.active {
        background: linear-gradient(135deg, #667eea, #764ba2);
        transform: scale(1.2);
    }

    .slide-enter {
        animation: slideInRight 0.5s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .slide-exit {
        animation: slideOutLeft 0.5s cubic-bezier(0.4, 0, 0.2, 1);
    }

    @keyframes slideInRight {
        from {
            opacity: 0;
            transform: translateX(50px);
        }
        to {
            opacity: 1;
            transform: translateX(0);
        }
    }

    @keyframes slideOutLeft {
        from {
            opacity: 1;
            transform: translateX(0);
        }
        to {
            opacity: 0;
            transform: translateX(-50px);
        }
    }
    
    .tooltip-section {
        display: flex;
        align-items: flex-start;
        gap: 5px; /* Reduced gap for tighter spacing */
        margin-bottom: 8px;
    }

    .tooltip-label {
        font-weight: 600;
        flex-shrink: 0;
        display: inline; /* Ensure inline behavior within flex */
        white-space: nowrap; /* Prevent unintended wrapping */
    }

    .tooltip-value {
        color: #e8f4fd;
        flex-grow: 1;
        display: inline; /* Ensure inline behavior within flex */
        white-space: normal; /* Allow wrapping for long values */
    }
        
    .status-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 500;
        text-transform: uppercase;
    }

    .status-ongoing {
        background-color: #48bb78;
        color: white;
    }

    .status-completed {
        background-color: #4299e1;
        color: white;
    }

    .status-hiatus {
        background-color: #ed8936;
        color: white;
    }

    .status-cancelled {
        background-color: #f56565;
        color: white;
    }

    @media (max-width: 768px) {
        .cover-item img {
            width: 120px;
            height: 180px;
        }
        .carousel-nav {
            width: 44px;
            height: 44px;
            font-size: 16px;
        }
        .cover-tooltip {
            width: 180px;
            margin-left: -90px;
            font-size: 12px;
        }
    }
    </style>
    """, unsafe_allow_html=True)

    # Enhanced navigation and cover display
    col_nav_left, col_covers, col_nav_right = st.columns([0.15, 3.7, 0.15])

    with col_nav_left:
        # Enhanced left arrow
        if index > 0:
            if st.button("‹", key="cover_prev", help="Previous covers"):
                st.session_state['cover_index'] = max(0, index - 3)
                st.rerun()
        else:
            st.markdown('<div class="carousel-nav disabled">‹</div>', unsafe_allow_html=True)

    with col_covers:
        if covers:
            # Get current 3 covers
            current_covers = covers[index:index + 3]

            cols = st.columns(3)
            for idx, cover in enumerate(current_covers):
                with cols[idx]:
                    # Parse genres
                    try:
                        if isinstance(cover['genres'], str):
                            genres = [g['value'] for g in json.loads(cover['genres'])] if cover['genres'] else []
                        else:
                            genres = cover['genres'] if cover['genres'] else []
                        genres_str = ", ".join(genres[:3]) if genres else "Unknown"
                        if len(genres) > 3:
                            genres_str += f" +{len(genres) - 3} more"
                    except Exception:
                        genres_str = "Unknown"

                    # Format status with badge
                    status = cover.get('status', 'Unknown').lower()
                    status_class = f"status-{status.replace(' ', '-')}"
                    if status not in ['ongoing', 'completed', 'hiatus', 'cancelled']:
                        status_class = "status-ongoing"  # Default styling

                    # Format tooltip content to match load_and_display_cover
                    tooltip_content = f"""
                    <div class="tooltip-section">
                        <span class="tooltip-label" style="color: #64b5f6;">Title:</span>
                        <span class="tooltip-value">{cover['title']}</span>
                    </div>
                    <div class="tooltip-section">
                        <span class="tooltip-label" style="color: #81c784;">Status:</span>
                        <span class="status-badge {status_class}">{cover.get('status', 'Unknown')}</span>
                    </div>
                    <div class="tooltip-section">
                        <span class="tooltip-label" style="color: #ffb74d;">Genres:</span>
                        <span class="tooltip-value">{genres_str}</span>
                    </div>
                    <div class="tooltip-section">
                        <span class="tooltip-label" style="color: #f48fb1;">Published:</span>
                        <span class="tooltip-value">{cover.get('published_year', 'Unknown')}</span>
                    </div>
                    """

                    # Display enhanced cover with tooltip
                    image_bytes = fetch_cover_image(cover["cover_url"])
                    if image_bytes is not None:
                        img_base64 = image_to_base64(image_bytes)
                        st.markdown(
                            f"""
                            <div class="cover-item">
                                <img src="data:image/jpeg;base64,{img_base64}" alt="{cover['title']}" loading="lazy">
                                <div class="cover-tooltip">{tooltip_content}</div>
                                <div class="cover-caption">{cover['title']}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

            # Add carousel indicators
            if len(covers) > 3:
                total_pages = (len(covers) + 2) // 3  # Ceiling division
                current_page = index // 3

                indicators_html = '<div class="carousel-indicators">'
                for i in range(total_pages):
                    active_class = "active" if i == current_page else ""
                    indicators_html += f'<div class="indicator-dot {active_class}"></div>'
                indicators_html += '</div>'

                st.markdown(indicators_html, unsafe_allow_html=True)
        else:
            st.info("🎨 No covers to display.")

    with col_nav_right:
        # Enhanced right arrow
        if index < max_index:
            if st.button("›", key="cover_next", help="Next covers"):
                st.session_state['cover_index'] = min(max_index, index + 3)
                st.rerun()
        else:
            st.markdown('<div class="carousel-nav disabled">›</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


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
    if st.session_state.year_filter:
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

    with tab2:
        st.header("Manga Analysis")
        col1, col2 = st.columns(2)
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
            if not st.session_state.selected_manga:
                scatter_data = load_chart_data(manga_filters, query_type="year_vs_mangas")
                fig_scatter = create_year_vs_mangas_histogram(scatter_data)
                if fig_scatter:
                    st.plotly_chart(fig_scatter, use_container_width=True)
                    charts['year_vs_chapters'] = fig_scatter
            else:
                st.info("No data for year vs. chapters scatter plot.")

        if not st.session_state.selected_manga:
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
            st.write(display_manga[display_cols])
        else:
            st.info("No manga data available.")
