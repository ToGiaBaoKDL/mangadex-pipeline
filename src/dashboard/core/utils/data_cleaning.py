# src/utils/data_cleaning.py
import streamlit as st
from datetime import datetime


def robust_decode(val):
    """Decode and clean text to handle invalid UTF-8 sequences."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    elif isinstance(val, str):
        return val.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
    return val


def clean_dataframe(df, columns):
    """Apply robust_decode to specified columns and optimize data types."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(robust_decode)
    for col in df.columns:
        if col.endswith('_id'):
            df[col] = df[col].astype('int32', errors='ignore')
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype('float32', errors='ignore')
    return df


def initialize_session_state():
    """Initialize session state variables."""
    defaults = {
        'favorites': set(),
        'manga_page': 0,
        'chapter_page': 0,
        'last_refresh': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'perf_stats': [],
        'status_filter': [],
        'genres_filter': [],
        'lang_filter': [],
        'original_language_filter': [],
        'year_filter': (2000, 2025),
        'sort_column': {'manga': None, 'chapter': None},
        'sort_order': {'manga': 'asc', 'chapter': 'asc'},
        'selected_manga': None
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
