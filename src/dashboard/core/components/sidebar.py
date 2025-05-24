import streamlit as st
from streamlit_searchbox import st_searchbox
from src.dashboard.core.database.postgres import load_filter_options, get_postgres_engine
from src.dashboard.core.utils.search import search_manga
from src.dashboard.core.config.config import load_config
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker


def render_sidebar():
    """Render the sidebar with controls."""
    config = load_config()
    with st.sidebar:
        st.header("📊 Dashboard Controls")

        # Data Controls
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh Data", use_container_width=True):
                engine = get_postgres_engine()
                if engine:
                    try:
                        Session = sessionmaker(bind=engine)
                        with Session() as session:
                            session.execute(text("""
                                UPDATE summary_metrics 
                                SET metric_value = (SELECT COUNT(*) FROM manga), 
                                    last_updated = NOW() 
                                WHERE metric_name = 'total_manga';
                            """))
                            session.execute(text("""
                                UPDATE summary_metrics 
                                SET metric_value = (SELECT COUNT(*) FROM chapter), 
                                    last_updated = NOW() 
                                WHERE metric_name = 'total_chapters';
                            """))
                            session.execute(text("""
                                UPDATE summary_metrics 
                                SET metric_value = (SELECT SUM(pages) FROM chapter), 
                                    last_updated = NOW() 
                                WHERE metric_name = 'total_images';
                            """))
                            session.execute(text("""
                                UPDATE summary_metrics 
                                SET metric_value = (SELECT AVG(pages) FROM chapter), 
                                    last_updated = NOW() 
                                WHERE metric_name = 'avg_pages_per_chapter';
                            """))
                            session.commit()
                        st.success("Metrics updated successfully!")
                    except Exception as e:
                        st.error(f"Error updating metrics: {str(e)}")
                st.cache_data.clear()
                st.session_state.last_refresh = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                st.rerun()
        with col2:
            if st.button("🗑️ Clear Cache", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("Cache cleared successfully!")

        # Search & Filters
        with st.expander("🔍 Search & Filters", expanded=True):
            selected_manga = st_searchbox(
                lambda x: search_manga(x, config["app"]["max_search_results"]),
                placeholder="Search manga title...",
                key="manga_search",
                help="Enter a manga title to filter the dataset."
            )
            st.session_state.selected_manga = selected_manga

            with st.form("filters_form"):
                status_options, lang_options, year_range, genre_options, language_options = load_filter_options()

                # Initialize year_filter if not set or if it's a tuple (legacy)
                if 'year_filter' not in st.session_state or isinstance(st.session_state.year_filter, tuple):
                    default_range = st.session_state.get('year_filter', year_range)
                    if isinstance(default_range, tuple):
                        default_range = list(default_range)
                    st.session_state.year_filter = {
                        'include_null': False,
                        'year_range': default_range
                    }

                # Year filter: checkbox for None and slider for range
                include_null_year = st.checkbox(
                    "Include Manga with No Published Year",
                    value=st.session_state.year_filter.get('include_null', False),
                    help="Check to include manga with no published year."
                )
                selected_year_range = st.slider(
                    "Published Year",
                    year_range[0],
                    year_range[1],
                    st.session_state.year_filter.get('year_range', year_range),
                    help="Select a year range to filter manga."
                )

                selected_status = st.multiselect(
                    "Manga Status",
                    status_options,
                    default=st.session_state.status_filter,
                    help="Select manga statuses to filter."
                )
                selected_genres = st.multiselect(
                    "Genres",
                    genre_options,
                    default=st.session_state.get('genres_filter', []),
                    help="Select genres to filter manga."
                )
                selected_lang = st.multiselect(
                    "Chapter Language",
                    lang_options,
                    default=st.session_state.lang_filter,
                    help="Select chapter languages to filter."
                )
                selected_orig_lang = st.multiselect(
                    "Original Language",
                    language_options,
                    default=st.session_state.get('original_language_filter', []),
                    help="Select original languages to filter manga."
                )

                col1, col2 = st.columns(2)
                with col1:
                    if st.form_submit_button("Apply Filters"):
                        st.session_state.manga_page = 0
                        st.session_state.chapter_page = 0
                        st.session_state.status_filter = selected_status
                        st.session_state.genres_filter = selected_genres
                        st.session_state.lang_filter = selected_lang
                        st.session_state.original_language_filter = selected_orig_lang
                        st.session_state.year_filter = {
                            'include_null': include_null_year,
                            'year_range': list(selected_year_range)
                        }
                        st.success("Filters applied!")
                with col2:
                    if st.form_submit_button("Reset Filters"):
                        st.session_state.manga_page = 0
                        st.session_state.chapter_page = 0
                        st.session_state.status_filter = status_options
                        st.session_state.genres_filter = genre_options
                        st.session_state.lang_filter = lang_options
                        st.session_state.original_language_filter = language_options
                        st.session_state.year_filter = {
                            'include_null': True,
                            'year_range': year_range
                        }
                        st.session_state.selected_manga = None
                        st.success("Filters reset!")

        # Feedback
        with st.expander("💬 Feedback", expanded=True):
            with st.form("feedback_form"):
                feedback = st.text_area("Suggestions or issues?", height=100)
                if st.form_submit_button("Submit"):
                    if feedback:
                        engine = get_postgres_engine()
                        if engine:
                            try:
                                pd.DataFrame([{'feedback_text': feedback}]).to_sql('user_feedback', engine,
                                                                                   if_exists='append', index=False)
                                st.success("Thank you for your feedback!")
                            except Exception as e:
                                st.error(f"Error saving feedback: {str(e)}")
                    else:
                        st.warning("Please enter feedback before submitting.")
