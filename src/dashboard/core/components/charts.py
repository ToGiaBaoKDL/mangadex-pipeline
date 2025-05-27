import plotly.express as px
import plotly.graph_objects as go
from scipy.signal import savgol_filter


def create_status_pie(df):
    """Create improved pie chart for manga status distribution."""
    if df.empty or 'status' not in df.columns or 'count' not in df.columns:
        return None

    fig = px.pie(
        df,
        names='status',
        values='count',
        title="📝 Manga Status Distribution",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        hole=0.4
    )

    fig.update_layout(
        title_font=dict(size=18, color='#FF7F00'),
        title_x=0.0,
        showlegend=True,
        legend_title_text='Status',
        margin=dict(t=80)
    )

    return fig


def create_genre_bar(df):
    """Create bar chart for top genres."""
    if df.empty or 'genre' not in df.columns:
        return None
    fig = px.bar(
        df,
        x='genre',
        y='count',
        title="🎭 Top 5 Genres",
        color='genre',
        color_discrete_sequence=px.colors.qualitative.Set2,
        text='count'
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Manga Count",
        showlegend=False,
        title_font=dict(size=18, color="#FF7F00"),
        uniformtext_mode='hide',
        margin=dict(t=80)
    )
    fig.update_traces(texttemplate='%{text}', textposition='outside')
    return fig


def create_year_vs_mangas_histogram(df):
    """Create histogram of manga count by publication year."""
    if df.empty or 'published_year' not in df.columns:
        return None

    grouped = df.groupby('published_year').size().reset_index(name='manga_count')
    grouped = grouped.sort_values('published_year')

    y_smooth = savgol_filter(grouped['manga_count'], window_length=5, polyorder=2)
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=grouped['published_year'],
        y=grouped['manga_count'],
        name='Manga Count',
        marker_color='rgba(100, 149, 237, 0.7)'
    ))

    fig.add_trace(go.Scatter(
        x=grouped['published_year'],
        y=y_smooth,
        mode='lines',
        name='Trend',
        line=dict(color='firebrick', width=2)
    ))

    fig.update_layout(
        title="📊 Manga Count by Year with Trendline",
        xaxis_title="Publication Year",
        yaxis_title="Manga Count",
        title_font=dict(size=18, color="#FF7F00")
    )

    return fig


def create_language_treemap(df):
    """Create treemap for original language distribution."""
    if df.empty or 'original_language' not in df.columns or 'count' not in df.columns:
        return None

    # Ensure unique leaves
    agg_df = df.groupby('original_language', dropna=False)['count'].sum().reset_index()
    agg_df = agg_df[agg_df['original_language'].notna()]
    agg_df = agg_df[agg_df['original_language'].astype(str).str.strip() != '']

    if agg_df.empty:
        return None

    fig = px.treemap(
        agg_df,
        path=['original_language'],
        values='count',
        title="🌐 Original Language Distribution",
        color='count',
        color_continuous_scale='Viridis',
        hover_data={'count': True, 'original_language': False},
        labels={'count': 'Manga Count'},
    )

    fig.update_layout(
        margin=dict(t=100, l=25, r=25, b=25),
        title_x=0.0,  # Align title to top-left
        font=dict(size=12),
        width=600,   # You can adjust width/height as needed
        height=400,
        title_font=dict(size=18, color="#FF7F00")
    )

    return fig


def create_genre_cooccurrence_heatmap(df):
    """Create heatmap for genre co-occurrence."""
    if df.empty or 'genre1' not in df.columns:
        return None
    pivot = df.pivot(index='genre1', columns='genre2', values='count').fillna(0)
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='Viridis',
        text=pivot.values,
        texttemplate="%{text}",
        textfont={"size": 10}
    ))
    fig.update_layout(
        title="🎭 Genre Co-occurrence Heatmap",
        xaxis_title="Genre",
        yaxis_title="Genre",
        xaxis=dict(tickangle=45),
        height=600,
        title_font=dict(size=18, color="#FF7F00")
    )
    return fig


def create_chapter_counts_bar(df):
    if df.empty or 'title' not in df.columns or 'chapter_count' not in df.columns:
        return None
    fig = px.bar(
        df,
        x='title',
        y='chapter_count',
        title="📖 Top Manga by Chapter Count",
        color='chapter_count',
        color_continuous_scale='Viridis',
        text='chapter_count'
    )
    fig.update_layout(
        xaxis_title="Manga Title",
        yaxis_title="Number of Chapters",
        xaxis_tickangle=45,
        showlegend=False,
        margin=dict(t=60, l=25, r=25, b=80),
        title_x=0.0,  # top-left
        title_font=dict(size=18, color="#FF7F00"),
        font=dict(size=12),
        uniformtext_minsize=8,
        uniformtext_mode='hide',
    )
    fig.update_traces(
        texttemplate='%{text}',
        textposition='outside',
        cliponaxis=False,
        hovertemplate="Manga: %{x}<br>Chapters: %{y}<extra></extra>",
        marker=dict(line=dict(color='#ffffff', width=1))
    )
    return fig
