import os
import numpy as np
import polars as pl
import pandas as pd
import gspread as gs
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()


@st.cache_data()
def load_data(sheet_name: str, sheet_id: str) -> pl.DataFrame:
    """Load specified sheet from Google Drive.

    Args:
        sheet_name (str): Name of the table to be loaded
        sheet_id (str): Google Drive Sheets ID Unique value stored in `.env` file.
        Environment variable is accessible with `dotenv.load_dotenv()` and `os.environ["<YOUR_SHEET_ID>"]`.

    Returns:
        pl.DataFrame: Polars DataFrame loaded from the specified Google Drive sheet.
    """

    # Construct the Google Drive URL using `sheet_id` and `sheet_name`
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    # Load the data from the URL as a Polars DataFrame
    # Fill Null values with `np.nan`
    df = pl.read_csv(
        url,
        columns=[
            "UID",
            "SUID",
            "NAME",
            "CHARACTER",
            "MAP",
            "PLACE",
            "PLAYERS",
            "DATE",
            "SEASON",
        ],
        dtypes={
            "UID": pl.Int64,
            "SUID": pl.Int64,
            "NAME": pl.String,
            "CHARACTER": pl.String,
            "MAP": pl.String,
            "PLACE": pl.Int64,
            "PLAYERS": pl.String,
            "DATE": pl.String,
            "SEASON": pl.Int64,
        },
    ).fill_nan(np.nan)

    return df


@st.cache_data()
def load_data_pd(sheet_name: str, sheet_id: str) -> pd.DataFrame:
    """Load specified sheet from Google Drive.

    Args:
        sheet_name (str): Name of the table to be loaded.
        sheet_id (str): Google Drive Sheets ID Unique value stored in `.env` file.
        Environment variable is accessible with `dotenv.load_dotenv()` and `os.environ["<YOUR_SHEET_ID>"]`.

    Returns:
        pd.DataFrame: Pandas DataFrame loaded from the specified Google Drive sheet.
    """

    # Construct the Google Drive URL using `sheet_id` and `sheet_name`
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    # Load the data from the URL as a Polars DataFrame
    # Fill Null values with `np.nan`
    df = pd.read_csv(
        url,
        usecols=[
            "UID",
            "SUID",
            "NAME",
            "CHARACTER",
            "MAP",
            "PLACE",
            "PLAYERS",
            "DATE",
            "SEASON",
        ],
    ).fillna(np.nan)

    return df


def create_gs_worksheet_connection(sheet_name: str, sheet_id: str) -> gs.Worksheet:
    """Create an edit session for specified Google Sheet. New records can be appended with the
    usage of `gspread.worksheet.Worksheet.append_rows()`.

    Args:
        sheet_name (str): Name of the table to be loaded.
        sheet_id (str): Google Drive Sheets ID Unique value stored in `.env` file.

    Returns:
        gs.Worksheet: Editable Google Sheet object.
    """

    # Initialize connection to Google Sheets Service Account
    gc = gs.service_account()

    # Construct the Google Drive URL using `sheet_id` and `sheet_name`
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    # Load specified Google Sheet based on constructed URL
    sh = gc.open_by_url(url)

    # Create an editable object from the loaded sheet
    # Records can be appended to source Google Sheet
    ws = sh.worksheet(sheet_name)

    return ws


df = load_data_pd(sheet_name="data", sheet_id=os.environ["SHEET_ID"])

st.markdown("# Welcome to Mario Kart! 🏎️")

st.markdown("## Games Played")

names = df["NAME"].value_counts()

st.write(names)

total_games_played_by_player_hist = px.histogram(data_frame=df, x="NAME").update_xaxes(
    categoryorder="total descending"
)

st.plotly_chart(total_games_played_by_player_hist)
