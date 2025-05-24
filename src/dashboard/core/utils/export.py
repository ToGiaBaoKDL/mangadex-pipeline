import streamlit as st
import pandas as pd
import base64
import io
import zipfile


def format_number(number):
    """Format number with thousands separators."""
    return f"{number:,}"


def export_data(df, filename, format_type):
    """Export data in multiple formats."""
    try:
        if format_type == "csv":
            csv = df.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            return f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">Download CSV</a>'
        elif format_type == "excel":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            b64 = base64.b64encode(output.getvalue()).decode()
            return (f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" '
                    f'download="{filename}.xlsx">Download Excel</a>')
        elif format_type == "parquet":
            output = io.BytesIO()
            df.to_parquet(output, index=False)
            b64 = base64.b64encode(output.getvalue()).decode()
            return (f'<a href="data:application/octet-stream;base64,{b64}" '
                    f'download="{filename}.parquet">Download Parquet</a>')
    except Exception as e:
        st.error(f"Error exporting data: {str(e)}")
        return ""


def export_charts(charts, filename):
    """Export all charts as PNGs in a ZIP file."""
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, (chart_name, fig) in enumerate(charts.items()):
            img_bytes = fig.write_image(format="png")
            zf.writestr(f"{chart_name}.png", img_bytes)
    b64 = base64.b64encode(output.getvalue()).decode()
    return f'<a href="data:application/zip;base64,{b64}" download="{filename}.zip">Download All Charts (ZIP)</a>'
