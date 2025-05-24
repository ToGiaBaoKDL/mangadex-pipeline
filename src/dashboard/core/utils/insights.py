from src.dashboard.core.utils.statistics import get_top_genres
from src.dashboard.core.database.postgres import get_postgres_engine
import streamlit as st
import pandas as pd
from sqlalchemy import text


def generate_insights():
    """Generate polished and well-formatted insights from the full manga and chapter tables."""
    insights = []
    engine = get_postgres_engine()
    if not engine:
        st.error("Database connection unavailable.")
        return ["ℹ️ No insights available due to database connection issues."]

    selected_manga = st.session_state.get('selected_manga')
    year_filter = st.session_state.get('year_filter', {'include_null': True, 'year_range': [1900, 2025]})
    params = {}

    try:
        with engine.connect() as conn:
            # Base query for manga with optional filters
            manga_conditions = []
            if selected_manga:
                manga_conditions.append("title = :title")
                params['title'] = selected_manga

            # Apply year filter
            if not selected_manga:  # Year filter only for global insights
                if year_filter['include_null'] and year_filter['year_range']:
                    manga_conditions.append(
                        f"(published_year BETWEEN :year_min AND :year_max OR published_year IS NULL)")
                    params['year_min'] = year_filter['year_range'][0]
                    params['year_max'] = year_filter['year_range'][1]
                elif year_filter['include_null']:
                    manga_conditions.append("published_year IS NULL")
                elif year_filter['year_range']:
                    manga_conditions.append(f"published_year BETWEEN :year_min AND :year_max")
                    params['year_min'] = year_filter['year_range'][0]
                    params['year_max'] = year_filter['year_range'][1]

            # Most Active Year (non-null)
            if not selected_manga:  # Skip for single manga
                # Build conditions for active year query (exclude null years)
                active_year_conditions = []
                active_year_params = {}

                if year_filter['year_range']:
                    active_year_conditions.append("published_year BETWEEN :year_min AND :year_max")
                    active_year_params['year_min'] = year_filter['year_range'][0]
                    active_year_params['year_max'] = year_filter['year_range'][1]

                # Always exclude null years for "most active year"
                active_year_conditions.append("published_year IS NOT NULL")

                active_year_where = " WHERE " + " AND ".join(
                    active_year_conditions) if active_year_conditions else " WHERE published_year IS NOT NULL"

                query = f"""
                SELECT published_year
                FROM manga
                {active_year_where}
                GROUP BY published_year
                ORDER BY COUNT(*) DESC, published_year DESC
                LIMIT 1
                """
                results_active_year = conn.execute(text(query), active_year_params)
                df = pd.DataFrame(results_active_year.fetchall())
                if not df.empty:
                    most_active_year = int(df['published_year'].iloc[0])
                    insights.append(f"**📅 Most Active Year:** In {most_active_year}, "
                                    f"the highest number of manga titles were published.")

            # Most Common Status
            status_conditions = manga_conditions + ["status IS NOT NULL"]
            status_where = " WHERE " + " AND ".join(
                status_conditions) if status_conditions else " WHERE status IS NOT NULL"

            query = f"""
            SELECT status, COUNT(*) as count
            FROM manga
            {status_where}
            GROUP BY status
            ORDER BY count DESC, status
            LIMIT 1
            """
            results_common_status = conn.execute(text(query), params)
            df = pd.DataFrame(results_common_status.fetchall())
            if not df.empty:
                top_status = df['status'].iloc[0]
                count = df['count'].iloc[0]
                if selected_manga:
                    insights.append(f"**📌 Status:** \"{top_status}\" "
                                    f"is the status of **{selected_manga}**.")
                else:
                    insights.append(f"**📌 Most Common Status:** \"{top_status}\" "
                                    f"is the most frequent, with **{count}** titles.")

            # Top Genre
            top_genres = get_top_genres(limit=1)
            if not top_genres.empty and 'genre' in top_genres.columns:
                top_genre = top_genres.iloc[0]['genre']
                count_manga = top_genres.iloc[0]['count']
                if selected_manga:
                    insights.append(f"**🎭 Top Genre:** **{top_genre}** is a key genre for **{selected_manga}**.")
                else:
                    insights.append(f"**🎭 Top Genre:** The most popular genre is **{top_genre}** with **{count_manga}** titles.")

            # Original Language
            lang_conditions = manga_conditions + ["original_language IS NOT NULL"]
            lang_where = " WHERE " + " AND ".join(
                lang_conditions) if lang_conditions else " WHERE original_language IS NOT NULL"

            query = f"""
            SELECT original_language, COUNT(*) as count
            FROM manga
            {lang_where}
            GROUP BY original_language
            ORDER BY count DESC, original_language
            LIMIT 1
            """
            results_original_lang = conn.execute(text(query), params)
            df = pd.DataFrame(results_original_lang.fetchall())
            if not df.empty:
                top_language = df['original_language'].iloc[0]
                if selected_manga:
                    insights.append(
                        f"**🈯 Original Language:** **{selected_manga}** was originally written in **{top_language}**.")
                else:
                    insights.append(
                        f"**🈯 Original Language:** Most manga were originally written in **{top_language}**.")

            # Dominant Chapter Language
            chapter_conditions = ["c.lang IS NOT NULL"]
            chapter_params = {}

            if selected_manga:
                chapter_conditions.append("m.title = :title")
                chapter_params['title'] = selected_manga

            chapter_where = " WHERE " + " AND ".join(chapter_conditions)

            query = f"""
            SELECT c.lang, COUNT(*) as count
            FROM chapter c
            JOIN manga m ON c.manga_id = m.manga_id
            {chapter_where}
            GROUP BY c.lang
            ORDER BY count DESC, c.lang
            LIMIT 1
            """
            results_dominant_lang = conn.execute(text(query), chapter_params)
            df = pd.DataFrame(results_dominant_lang.fetchall())
            if not df.empty:
                dominant_lang = df['lang'].iloc[0]
                if selected_manga:
                    insights.append(
                        f"**🗣️ Dominant Language:** The most common chapter language for **{selected_manga}** is **{dominant_lang}**.")
                else:
                    insights.append(
                        f"**🗣️ Dominant Language:** The most common chapter language is **{dominant_lang}**.")

            # Trending Manga
            trending_conditions = manga_conditions + ["updated_at IS NOT NULL"]
            trending_where = " WHERE " + " AND ".join(
                trending_conditions) if trending_conditions else " WHERE updated_at IS NOT NULL"

            query = f"""
            SELECT title
            FROM manga
            {trending_where}
            ORDER BY updated_at DESC
            LIMIT 1
            """
            results_trending_manga = conn.execute(text(query), params)
            df = pd.DataFrame(results_trending_manga.fetchall())
            if not df.empty:
                trending_manga = df['title'].iloc[0]
                if selected_manga:
                    insights.append(f"**🔥 Trending Manga:** **{trending_manga}** was recently updated.")
                else:
                    insights.append(
                        f"**🔥 Trending Manga:** The most recently updated title is **\"{trending_manga}\"**.")

    except Exception as e:
        st.error(f"Error generating insights: {str(e)}")
        return ["ℹ️ No insights available due to database error."]

    return insights if insights else ["ℹ️ No insights available due to limited data."]
