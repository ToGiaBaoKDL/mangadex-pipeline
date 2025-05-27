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
    st.session_state.setdefault('status_filter', [])
    st.session_state.setdefault('genres_filter', [])
    st.session_state.setdefault('original_language_filter', [])
    st.session_state.setdefault('selected_manga', None)

    if 'initialized' not in st.session_state:
        status_options, year_range, genre_options, language_options = load_filter_options()
        st.session_state.published_year = {
            'include_null': True,
            'year_range': year_range
        }
        st.session_state.initialized = True
    else:
        # Ensure published_year exists
        st.session_state.setdefault('published_year', {
            'include_null': True,
            'year_range': [1900, 2025]
        })

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
            # Manga title searchbox
            st.info("Other filters are disabled while a manga is selected.")
            selected_manga = st_searchbox(
                lambda x: search_manga(x, config["app"]["max_search_results"]),
                placeholder="Search manga title...",
                key="manga_search",
                help="Enter a manga title to filter the dataset.",
            )
            st.session_state.selected_manga = selected_manga

            with st.form("filters_form"):
                status_options, year_range, genre_options, language_options = load_filter_options()

                # Initialize published_year if not set or if it's a tuple (legacy)
                if 'published_year' not in st.session_state or isinstance(st.session_state.published_year, tuple):
                    default_range = st.session_state.get('published_year', year_range)
                    if isinstance(default_range, tuple):
                        default_range = list(default_range)
                    st.session_state.published_year = {
                        'include_null': True,
                        'year_range': default_range
                    }

                include_null_year = st.checkbox(
                    "Include Manga with No Published Year",
                    value=st.session_state.published_year.get('include_null', True),
                    key="include_null_year",
                    help="Check to include manga with no published year."
                )
                selected_year_range = st.slider(
                    "Published Year",
                    year_range[0],
                    year_range[1],
                    st.session_state.published_year.get('year_range', year_range),
                    key="year_range_slider",
                    help="Select a year range to filter manga."
                )

                selected_status = st.multiselect(
                    "Manga Status",
                    status_options,
                    default=None if st.session_state.status_filter == [] else st.session_state.status_filter,
                    key="status_multiselect",
                    help="Select manga statuses to filter."
                )
                selected_genres = st.multiselect(
                    "Genres",
                    genre_options,
                    default=None if st.session_state.genres_filter == [] else st.session_state.genres_filter,
                    key="genres_multiselect",
                    help="Select genres to filter manga."
                )
                selected_orig_lang = st.multiselect(
                    "Original Language",
                    language_options,
                    default=None if st.session_state.original_language_filter == [] else st.session_state.original_language_filter,
                    key="orig_lang_multiselect",
                    help="Select original languages to filter manga."
                )

                col1, col2 = st.columns(2)
                with col1:
                    if st.form_submit_button("Apply Filters"):
                        st.session_state.status_filter = selected_status
                        st.session_state.genres_filter = selected_genres
                        st.session_state.original_language_filter = selected_orig_lang
                        st.session_state.published_year = {
                            'include_null': include_null_year,
                            'year_range': list(selected_year_range)
                        }
                        st.cache_data.clear()
                        st.success("Filters applied!")
                        st.rerun()
                with col2:
                    if st.form_submit_button("Reset Filters"):
                        st.session_state.status_filter = []
                        st.session_state.genres_filter = []
                        st.session_state.original_language_filter = []
                        st.session_state.published_year = {
                            'include_null': True,
                            'year_range': year_range
                        }
                        st.session_state.selected_manga = None
                        st.session_state.pop("manga_search", None)
                        st.cache_data.clear()
                        st.success("Filters reset to original state!")
                        st.rerun()

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
        # st.write(st.session_state.manga_search)
