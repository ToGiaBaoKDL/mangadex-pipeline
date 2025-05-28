import streamlit as st
import requests
import base64
import json
import pandas as pd
from sqlalchemy import text
from src.dashboard.core.database.postgres import get_postgres_engine


def fetch_cover_image(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Referer": "https://uploads.mangadex.org/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
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


@st.fragment
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

    # Enhanced navigation and cover display
    col_nav_left, col_covers, col_nav_right = st.columns([0.15, 3.7, 0.15])

    with col_nav_left:
        # Enhanced left arrow
        if index > 0:
            if st.button("‹", key="cover_prev", help="Previous covers"):
                st.session_state['cover_index'] = max(0, index - 3)
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
                # st.rerun()
        else:
            st.markdown('<div class="carousel-nav disabled">›</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


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
