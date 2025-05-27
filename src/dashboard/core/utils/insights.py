from src.dashboard.core.database.postgres import get_postgres_engine
import streamlit as st
import pandas as pd
from sqlalchemy import text
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import os
import re
import json
from tavily import TavilyClient


load_dotenv()
GEMINI_MODEL = "gemini-2.0-flash-lite"


def get_filtered_manga_count(manga_filters):
    """Return the count of manga matching the filters and total manga count."""
    engine = get_postgres_engine()
    if not engine:
        return 0, 0
    try:
        params = {}
        conditions = []
        if manga_filters:
            for k, v in manga_filters.items():
                if k == 'published_year' and v.get('year_range'):
                    if v['include_null']:
                        conditions.append("(published_year BETWEEN :year_min AND :year_max OR published_year IS NULL)")
                    else:
                        conditions.append("published_year BETWEEN :year_min AND :year_max")
                    params['year_min'] = v['year_range'][0]
                    params['year_max'] = v['year_range'][1]
                elif k == 'genres' and not v:
                    placeholders = ','.join([f':g{i}' for i in range(len(v))])
                    conditions.append(f"EXISTS (SELECT 1 FROM unnest(genres) g WHERE g IN ({placeholders}))")
                    for i, val in enumerate(v):
                        params[f'g{i}'] = val
                elif k == 'status' and v:
                    placeholders = ','.join([f':s{i}' for i in range(len(v))])
                    conditions.append(f"status IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f's{i}'] = val
                elif k == 'original_language' and v:
                    placeholders = ','.join([f':ol{i}' for i in range(len(v))])
                    conditions.append(f"original_language IN ({placeholders})")
                    for i, val in enumerate(v):
                        params[f'ol{i}'] = val
                elif k == 'title' and v:
                    conditions.append("title = :title")
                    params['title'] = v

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        query = f"SELECT COUNT(*) as count FROM manga {where_clause}"
        total_query = "SELECT COUNT(*) as count FROM manga"
        with engine.connect() as conn:
            filter_results = conn.execute(text(query), params)
            total_results = conn.execute(text(total_query))
            filtered_count = pd.DataFrame(filter_results.fetchall(), columns=filter_results.keys())['count'].iloc[0]
            total_count = pd.DataFrame(total_results.fetchall(), columns=total_results.keys())['count'].iloc[0]

        return int(filtered_count), int(total_count)
    except Exception as e:
        st.error(f"Error counting filtered manga: {str(e)}")
        return 0, 0


def summarize_filtered_data(manga_filters=None):
    """Summarize filtered manga and chapter data for LLM insight generation."""
    engine = get_postgres_engine()
    if not engine:
        return None

    summary = {}
    params = {}
    conditions = []
    if manga_filters:
        for k, v in manga_filters.items():
            if k == 'published_year' and v.get('year_range'):
                if v['include_null']:
                    conditions.append("(published_year BETWEEN :year_min AND :year_max OR published_year IS NULL)")
                else:
                    conditions.append("published_year BETWEEN :year_min AND :year_max")
                params['year_min'] = v['year_range'][0]
                params['year_max'] = v['year_range'][1]
            elif k == 'genres' and v:
                placeholders = ','.join([f':g{i}' for i in range(len(v))])
                conditions.append(f"EXISTS (SELECT 1 FROM unnest(genres) g WHERE g IN ({placeholders}))")
                for i, val in enumerate(v):
                    params[f'g{i}'] = val
            elif k == 'status' and v:
                placeholders = ','.join([f':s{i}' for i in range(len(v))])
                conditions.append(f"status IN ({placeholders})")
                for i, val in enumerate(v):
                    params[f's{i}'] = val
            elif k == 'original_language' and v:
                placeholders = ','.join([f':ol{i}' for i in range(len(v))])
                conditions.append(f"original_language IN ({placeholders})")
                for i, val in enumerate(v):
                    params[f'ol{i}'] = val
    manga_where = " WHERE " + " AND ".join(conditions) if conditions else ""

    try:
        with engine.connect() as conn:
            # Total counts
            filtered_count, total_manga = get_filtered_manga_count(manga_filters)
            summary['total_manga'] = filtered_count
            summary['total_manga_all'] = total_manga
            query = f"SELECT COUNT(*) as count FROM chapter c JOIN manga m ON c.manga_id = m.manga_id {manga_where}"
            total_chapters = pd.DataFrame(conn.execute(text(query), params).fetchall())['count'].iloc[0]
            summary['total_chapters'] = int(total_chapters)

            # Top genres
            query = f"""
            SELECT trim(g) as genre, COUNT(*) as count
            FROM manga m, unnest(genres) g
            {manga_where}
            GROUP BY genre
            ORDER BY count DESC
            LIMIT 3
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            summary['genres'] = [
                {'name': row['genre'], 'count': int(row['count']), 'percent': (row['count'] / filtered_count * 100) if filtered_count else 0}
                for _, row in df.iterrows()
            ]

            # Top statuses
            query = f"""
            SELECT m.status, COUNT(*) as count
            FROM manga m
            {manga_where}
            GROUP BY m.status
            ORDER BY count DESC
            LIMIT 3
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            summary['statuses'] = [
                {'name': row['status'], 'count': int(row['count']), 'percent': (row['count'] / filtered_count * 100) if filtered_count else 0}
                for _, row in df.iterrows()
            ]

            # Top original languages
            query = f"""
            SELECT m.original_language, COUNT(*) as count
            FROM manga m
            {manga_where}
            GROUP BY m.original_language
            ORDER BY count DESC
            LIMIT 3
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            summary['original_languages'] = [
                {'name': row['original_language'], 'count': int(row['count']), 'percent': (row['count'] / filtered_count * 100) if filtered_count else 0}
                for _, row in df.iterrows()
            ]

            # Publication years
            query = f"""
            SELECT MIN(published_year) as min_year, MAX(published_year) as max_year
            FROM manga m
            {manga_where}
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            summary['year_range'] = {
                'min': int(df['min_year'].iloc[0]) if pd.notna(df['min_year'].iloc[0]) else None,
                'max': int(df['max_year'].iloc[0]) if pd.notna(df['max_year'].iloc[0]) else None
            }
            query = f"""
            SELECT published_year, COUNT(*) as count
            FROM manga m
            {manga_where}
            GROUP BY published_year
            ORDER BY count DESC
            LIMIT 2
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            summary['top_years'] = [
                {'year': int(row['published_year']) if pd.notna(row['published_year']) else 'NULL', 'count': int(row['count'])}
                for _, row in df.iterrows()
            ]

            # Most recent update
            query = f"""
            SELECT m.title, m.updated_at
            FROM manga m
            {manga_where}
            ORDER BY m.updated_at DESC
            LIMIT 1
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            if not df.empty:
                summary['recent_update'] = {
                    'title': df['title'].iloc[0],
                    'date': df['updated_at'].iloc[0].strftime('%Y-%m-%d')
                }

            # Manga with most chapters
            query = f"""
            SELECT m.title, COUNT(c.chapter_id) as count
            FROM manga m
            LEFT JOIN chapter c ON m.manga_id = c.manga_id
            {manga_where}
            GROUP BY m.title
            ORDER BY count DESC
            LIMIT 1
            """
            df = pd.DataFrame(conn.execute(text(query), params).fetchall())
            if not df.empty:
                summary['most_chapters'] = {
                    'title': df['title'].iloc[0],
                    'count': int(df['count'].iloc[0])
                }

    except Exception as e:
        st.error(f"Error summarizing data: {str(e)}")
        return None

    return summary, manga_filters


def search_manga_info(manga_title: str) -> dict:
    """Search for additional manga information using Tavily Search API."""
    try:
        client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
        response = client.search(
            query=f"manga {manga_title} synopsis author and information",
            search_depth="basic",
            max_results=2,
            include_domains=["https://mangadex.org/", "https://www.mangaread.org/"]
        )

        search_data = {
            "synopsis": None,
            "author": None,
            "popularity": None,
            "additional_info": []
        }

        # Extract relevant information from search results
        for result in response.get("results", []):
            content = result.get("content", "")
            title = result.get("title", "").lower()

            # Extract synopsis
            if not search_data["synopsis"] and "synopsis" in title.lower():
                search_data["synopsis"] = content[:500]  # Limit length

            # Extract author
            if not search_data["author"] and "author" in content.lower():
                match = re.search(r"author:\s*([^.,]+)", content, re.IGNORECASE)
                if match:
                    search_data["author"] = match.group(1).strip()

            # Extract popularity or rankings
            if not search_data["popularity"] and any(kw in content.lower() for kw in ["ranked", "popularity", "members"]):
                search_data["popularity"] = content[:200]

            # Collect additional info
            if content and len(search_data["additional_info"]) < 3:
                search_data["additional_info"].append(content[:200])

        return search_data
    except Exception as e:
        st.warning(f"Failed to fetch external data for {manga_title}: {str(e)}")
        return {
            "synopsis": None,
            "author": None,
            "popularity": None,
            "additional_info": []
        }


def generate_insights(manga_filters=None, selected_manga=None):
    """Generate insights using an LLM based on filtered data summary."""
    summary, manga_filters = summarize_filtered_data(manga_filters)
    if not summary:
        return False, ["No insights available due to data retrieval issues."]

    if summary['total_manga'] == 0:
        return False, ["No manga match the current filters."]

    if summary['total_manga'] == 1 and selected_manga:
        # Single manga case: Fetch external data and generate insights
        manga_title = selected_manga
        search_data = search_manga_info(manga_title)

        def build_prompt_single_manga(selected_manga, search_data) -> str:
            # Format search data
            search_summary = []
            if search_data["synopsis"]:
                search_summary.append(f"- Synopsis: {search_data['synopsis']}")
            if search_data["author"]:
                search_summary.append(f"- Author: {search_data['author']}")
            if search_data["popularity"]:
                search_summary.append(f"- Popularity: {search_data['popularity']}")
            if search_data["additional_info"]:
                search_summary.append(f"- Additional Info: {'; '.join(search_data['additional_info'])}")

            return f"""You are an assistant that writes short 2-3 sentences, 
            interesting facts about a manga based on the manga title provided below.
            
            Your task:
            - Generate 4–5 unique insights or trivia about this manga.
            - Use your own knowledge if needed. Avoid hallucination.
            - Return the output as a **valid JSON array only** (no extra text, no comments).
            - Each item must follow this schema:
              {{
                "text": "...",
                "tooltip": "...",  // One of: "Publication", "Status", "Genre", "Language", "Chapter", "Update", "General"
                "icon": "..."      // Emoji representing the tooltip
              }}
            Try to don't have duplicate category in the response.
            Wrap the entire JSON array in triple backticks with 'json' for proper formatting:

            ```json
            **Manga Info**:
            {selected_manga}
            **External Search Data**:
            {chr(10).join(search_summary) if search_summary else '- None'}
            """

        llm = ChatGoogleGenerativeAI(model=GEMINI_MODEL, temperature=0.1)
        response = llm.invoke(build_prompt_single_manga(selected_manga, search_data))
        content = getattr(response, 'content', str(response))
        json_str = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
        if json_str:
            content = json_str.group(1)

        try:
            parsed_insights = json.loads(content)
        except json.JSONDecodeError as e:
            st.error(f"Failed to parse LLM response: {e}")
            return False, ["Error generating insights for single manga."]

        return True, parsed_insights

    # Multiple manga case: Use existing logic for summary-based insights
    top_genres = ', '.join([
        f"{g['name']} ({g['count']} manga, {g['percent']:.1f}%)"
        for g in summary['genres']
    ])
    top_statuses = ', '.join([
        f"{s['name']} ({s['count']} manga, {s['percent']:.1f}%)"
        for s in summary['statuses']
    ])
    top_languages = ', '.join([
        f"{l['name']} ({l['count']} manga, {l['percent']:.1f}%)"
        for l in summary['original_languages']
    ])
    top_years = ', '.join([
        f"{y['year']} ({y['count']} manga)"
        for y in summary['top_years']
    ])

    data_summary = [
        f"- Total manga: {summary['total_manga']} ({summary['total_manga_all']} in full dataset)",
        f"- Total chapters: {summary['total_chapters']}",
        f"- Top genres: {top_genres}",
        f"- Top statuses: {top_statuses}",
        f"- Top original languages: {top_languages}",
        f"- Publication years: {summary['year_range']['min'] or 'N/A'}–{summary['year_range']['max'] or 'N/A'}, most active: {top_years}",
    ]

    if 'recent_update' in summary:
        data_summary.append(
            f"- Most recent manga update: \"{summary['recent_update']['title']}\" on {summary['recent_update']['date']}"
        )
    if 'most_chapters' in summary:
        data_summary.append(
            f"- Manga with most chapters: \"{summary['most_chapters']['title']}\" ({summary['most_chapters']['count']} chapters)"
        )

    filters_desc = []
    if manga_filters:
        if 'status_filter' in manga_filters:
            filters_desc.append(f"Status: {', '.join(manga_filters['status_filter'])}")
        if 'genres_filter' in manga_filters:
            filters_desc.append(f"Genres: {', '.join(manga_filters['genres_filter'])}")
        if 'language_filter' in manga_filters:
            filters_desc.append(f"Original language: {', '.join(manga_filters['language_filter'])}")
        if 'year_filter' in manga_filters and manga_filters['year_filter'].get('year_range'):
            year_range = manga_filters['year_filter']['year_range']
            null_text = " (include NULL years)" if manga_filters['year_filter']['include'] else ""
            filters_desc.append(f"Year range: {year_range[0]}–{year_range[1]}{null_text}")
        if 'title' in manga_filters:
            filters_desc.append(f"Title: {manga_filters['title']}")

    CATEGORY_CONFIG = {
        "publication": {"icon": "📅", "tooltip": "Publication year insights"},
        "status": {"icon": "📝", "tooltip": "Manga status insights"},
        "genre": {"icon": "🎭", "tooltip": "Genre insights"},
        "language": {"icon": "🌐", "tooltip": "Language insights"},
        "chapter": {"icon": "📖", "tooltip": "Chapter insights"},
        "update": {"icon": "🆕", "tooltip": "Recent activity insights"},
        "general": {"icon": "ℹ️", "tooltip": "General insight"},
    }

    def build_prompt(data_summary: list, filters_desc: list) -> str:
        """Build prompt for LLM to generate insights for multiple manga."""
        filters_str = '\n'.join(f'- {f}' for f in filters_desc) if filters_desc else '- None'
        return f"""You are an analytics assistant for a manga database.
        Below is a summary of filtered manga statistics, along with applied filters.
        Generate 5–8 concise insights (1–2 sentences each) highlighting key patterns, trends, or anomalies.
        Ensure insights are grounded in the provided data and avoid hallucination.
        Each insight must include a "category" field with one of: 
        "publication", "status", "genre", "language", "chapter", "update", "general".

        Return a JSON array of objects with:
        - "text": the insight sentence,
        - "category": the category.

        Example:
        ```json
        [
          {{"text": "Most manga were published after 2018.", "category": "publication"}},
          {{"text": "Ongoing manga have higher chapter counts.", "category": "status"}}
        ]
        ```

        **Data Summary**:
        {chr(10).join(data_summary)}

        **Filters**:
        {filters_str}
        """

    def parse_json_response(content: str) -> list:
        """Parse JSON response from LLM."""
        try:
            match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            json_str = match.group(1) if match else content.strip()
            parsed = json.loads(json_str)
            return parsed if isinstance(parsed, list) else []
        except Exception as e:
            st.error(f"Failed to parse insights: {e}")
            return []

    def enrich_insights(insights: list) -> list:
        """Add icons and tooltips to insights."""
        enriched = []
        for item in insights:
            cat = item.get("category", "general").strip().lower()
            config = CATEGORY_CONFIG.get(cat, CATEGORY_CONFIG["general"])
            enriched.append({
                "text": item["text"],
                "icon": config["icon"],
                "tooltip": config["tooltip"]
            })
        return enriched

    def generate_structured_insights(data_summary: list, filters_desc: list) -> list:
        """Generate insights for multiple manga using LLM."""
        prompt = build_prompt(data_summary, filters_desc)
        try:
            llm = ChatGoogleGenerativeAI(model=GEMINI_MODEL, temperature=0.1)
            response = llm.invoke(prompt)
            content = getattr(response, 'content', str(response))
            parsed = parse_json_response(content)
            return enrich_insights(parsed)
        except Exception as e:
            st.error(f"Error generating insights: {e}")
            return []

    # Generate insights for multiple manga
    insight_configs = generate_structured_insights(data_summary, filters_desc)
    return True, insight_configs
