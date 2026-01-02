import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
from pathlib import Path
import os

# --- Get current directory for relative paths ---
current_dir = Path(__file__).parent

# --- Download Helper Functions ---
def df_to_csv_download(df, filename):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button('Download Data', csv, file_name=filename, mime='text/csv')

# --- Caching for performance ---
@st.cache_data
def load_csv(filename, **kwargs):
    try:
        # Try relative path first
        file_path = current_dir / filename
        if file_path.exists():
            return pd.read_csv(file_path, **kwargs)
        
        # Try absolute path for compatibility
        if os.path.exists(filename):
            return pd.read_csv(filename, **kwargs)
            
        # Try without directory prefix
        simple_filename = os.path.basename(filename)
        simple_path = current_dir / simple_filename
        if simple_path.exists():
            return pd.read_csv(simple_path, **kwargs)
            
        st.warning(f"File not found: {filename}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading {filename}: {e}")
        return pd.DataFrame()

# Load data files with error handling
dashboard_dir = current_dir / 'dashboard'
if not dashboard_dir.exists():
    dashboard_dir = current_dir  # Fallback to current directory

low_inventory_df = load_csv(dashboard_dir / 'Low Inventory.csv')
overstock_df = load_csv(dashboard_dir / 'Overstock.csv')
turnover_df = load_csv(dashboard_dir / 'Inventory Turnover Rate.csv')
forecast_deviation_df = load_csv(dashboard_dir / 'Forecast Deviation.csv')
snapshot_df = load_csv(dashboard_dir / 'Inventory Snapshot.csv')

# Load forecast sample with error handling
try:
    forecast_path = dashboard_dir / 'Forecast.csv'
    if forecast_path.exists():
        forecast_sample = pd.read_csv(forecast_path, nrows=5000, header=None)
        forecast_header = [
            'date', 'store_id', 'product_id', 'actual', 'forecast', 'error', 'accuracy_class',
            'price', 'discount', 'holiday_promotion', 'weather_condition', 'seasonality'
        ]
        forecast_sample.columns = forecast_header[:len(forecast_sample.columns)]
    else:
        forecast_sample = pd.DataFrame()
        st.info("Forecast.csv not found - some visualizations may be unavailable")
except Exception as e:
    st.warning(f"Error loading Forecast.csv: {e}")
    forecast_sample = pd.DataFrame()

# --- Check if we have any data ---
if all(df.empty for df in [low_inventory_df, overstock_df, turnover_df, forecast_deviation_df, snapshot_df]):
    st.error("""
    No data files found! Please ensure your data files are in the correct location:
    
    Expected files:
    - Low Inventory.csv
    - Overstock.csv
    - Inventory Turnover Rate.csv
    - Forecast Deviation.csv
    - Inventory Snapshot.csv
    
    Current directory: {}
    """.format(current_dir))
    st.stop()

# --- Rest of your dashboard code continues unchanged from here ---
# --- Sidebar Filters ---
st.sidebar.title('Filters')

# Date range filter (for snapshot_df and forecast_sample)
min_date = None
max_date = None
if not snapshot_df.empty and 'date' in snapshot_df.columns:
    snapshot_df['date'] = pd.to_datetime(snapshot_df['date'], errors='coerce', dayfirst=True)
    min_date = snapshot_df['date'].min()
    max_date = snapshot_df['date'].max()
    date_range = st.sidebar.date_input('Select Date Range', [min_date, max_date], min_value=min_date, max_value=max_date)
else:
    date_range = None

# Store filter
def get_unique_stores():
    stores = set()
    for df in [snapshot_df, turnover_df, low_inventory_df, overstock_df]:
        if not df.empty and 'store_id' in df.columns:
            stores.update(df['store_id'].dropna().unique())
    return sorted(list(stores))

store_options = get_unique_stores()
selected_stores = st.sidebar.multiselect('Select Store(s)', store_options, default=store_options)

# Product filter
def get_unique_products():
    products = set()
    for df in [snapshot_df, turnover_df, low_inventory_df, overstock_df]:
        if not df.empty and 'product_id' in df.columns:
            products.update(df['product_id'].dropna().unique())
    return sorted(list(products))

product_options = get_unique_products()
selected_products = st.sidebar.multiselect('Select Product(s)', product_options, default=product_options)

# Theme toggle
theme = st.sidebar.radio('Theme', ['Light', 'Dark'], index=0)
if theme == 'Dark':
    st.markdown('<style>body { background-color: #222; color: #eee; } .stApp { background-color: #222; } </style>', unsafe_allow_html=True)
    kpi_css = """
    <style>
    .kpi {
        background: #333;
        border-radius: 8px;
        padding: 1em 0.5em 0.8em 0.5em;
        margin-bottom: 0.5em;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        width: 100%;
        box-sizing: border-box;
        min-width: 0;
        max-width: 100%;
    }
    .kpi-title { font-size: 1.1em; color: #eee; }
    .kpi-value { font-size: 2em; font-weight: bold; color: #fff; }
    [data-testid="column"] { padding-right: 0.5em; }
    </style>
    """
else:
    kpi_css = """
    <style>
    .kpi {
        background: #f5f5f5;
        border-radius: 8px;
        padding: 1em 0.5em 0.8em 0.5em;
        margin-bottom: 0.5em;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        width: 100%;
        box-sizing: border-box;
        min-width: 0;
        max-width: 100%;
    }
    .kpi-title { font-size: 1.1em; color: #888; }
    .kpi-value { font-size: 2em; font-weight: bold; color: #222; }
    [data-testid="column"] { padding-right: 0.5em; }
    </style>
    """
st.markdown(kpi_css, unsafe_allow_html=True)

# Continue with the rest of your existing dashboard code...