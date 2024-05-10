import os
import pandas as pd
import polars as pl
import streamlit as st
import gspread as gs
from datetime import datetime
from home import load_data
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()


df = load_data(sheet_name="data", sheet_id=os.environ["SHEET_ID"])

gc = gs.service_account()
sheet_id = os.environ["SHEET_ID"]
sheet_name = "data"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
sh = gc.open_by_url(url)
ws = sh.worksheet("data")

st.markdown("## Enter the Stats Nerd")

names_unique = df["NAME"].unique()
map_names_unique = df["MAP"].unique()
char_names_unique = df["CHARACTER"].unique()

last_suid = int(df.select(pl.last("SUID")).item())
last_uid = df.select(pl.last("UID")).item()
last_season = df.select(pl.last("SEASON")).item()

# idx = df[df["SEASON"] == (last_season - 1)][-1]
# print(idx)

# df.filter(pl.col("SEASON") == (last_season - 1)).select(pl.tail(df.columns, 1))

with st.form("Enter Stats"):
    st.write(f"Previous SUID: {last_suid}")
    suid = st.number_input(label="Current SUID", min_value=last_suid)

    st.write("Positions")

    # first_place = st.selectbox(label="1st", options=names_unique)
    # second_place = st.selectbox(label="2nd", options=names_unique)
    # third_place = st.selectbox(label="3rd", options=names_unique)
    # fourth_place = st.selectbox(label="4th", options=names_unique)

    players_write = st.multiselect(label="Players", options=names_unique)
    characters_write = st.multiselect(label="Characters", options=char_names_unique)

    st.write("Map")
    mk_map = st.radio(label="Map", options=map_names_unique)

    sub = st.form_submit_button("Submit")

    if sub:
        st.write("Picked", players_write)
        data = pd.DataFrame(
            data={
                "NAME": players_write,
                "CHARACTER": characters_write,
            }
        )
        data = data.dropna(axis=0, how="any")

        data["UID"] = last_uid + 1
        data["MAP"] = mk_map
        data["PLACE"] = range(1, (1 + data.shape[0]), 1)
        data["SUID"] = suid

        data["PLAYERS"] = data.shape[0]
        data["DATE"] = datetime.now()
        data["DATE"] = data["DATE"].astype(str)

        data = data[
            ["UID", "SUID", "NAME", "CHARACTER", "MAP", "PLACE", "PLAYERS", "DATE"]
        ]

        st.dataframe(data=data)

        concat_df = pd.concat(objs=[df, data]).reset_index(drop=True)
        st.write(concat_df.tail())

        dl = data.values.tolist()
        ws.append_rows(dl)
