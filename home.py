import os
import numpy as np
import polars as pl
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
    df = pl.read_csv(url).fill_null(np.nan)

    return df


df = load_data(sheet_name="data", sheet_id=os.environ["SHEET_ID"])

st.markdown("# Welcome to Mario Kart! 🏎️")

st.markdown("## Games Played")

names = pl.DataFrame(df["NAME"].value_counts())

st.write(names)

total_games_played_by_player_hist = px.histogram(data_frame=df, x="NAME").update_xaxes(
    categoryorder="total descending"
)

st.plotly_chart(total_games_played_by_player_hist)
