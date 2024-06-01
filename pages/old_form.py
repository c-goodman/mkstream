import os
import pandas as pd
import polars as pl
import streamlit as st
import gspread as gs
from datetime import datetime
from home import load_data_pd, create_gs_worksheet_connection
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()


df = load_data_pd(sheet_name="data", sheet_id=os.environ["SHEET_ID"])

ws = create_gs_worksheet_connection(sheet_name="data", sheet_id=os.environ["SHEET_ID"])

st.markdown("## Enter the Stats Nerd")

names_unique = df["NAME"].value_counts().index.tolist()
map_names_unique = df["MAP"].value_counts().index.tolist()
char_names_unique = df["CHARACTER"].value_counts().index.tolist()

last_suid = df["SUID"].iloc[-1]
last_uid = df["UID"].iloc[-1]
last_season = df["SEASON"].iloc[-1]

# df.groupby(["SUID"]).agg(pd.NamedAgg("NAME", "first"))


with st.form("Enter Stats"):

    st.write(f"Previous SUID: {last_suid}")
    suid = st.number_input(
        label="Current SUID", min_value=last_suid, max_value=(last_suid + 1)
    )

    st.write("Positions")

    # first_place = st.selectbox(label="1st", options=names_unique)
    # second_place = st.selectbox(label="2nd", options=names_unique)
    # third_place = st.selectbox(label="3rd", options=names_unique)
    # fourth_place = st.selectbox(label="4th", options=names_unique)

    players_write = st.multiselect(
        label="Players", options=names_unique, max_selections=4
    )

    characters_write = st.multiselect(
        label="Characters", options=char_names_unique, max_selections=4
    )

    if len(players_write) != len(characters_write):
        st.write("Length of Selected Players and Characters do not match!")
        st.write("Fix it you big nerd")

    st.write("Map")
    mk_map = st.radio(label="Map", options=map_names_unique)

    new_data = pd.DataFrame(
        data={
            "UID": last_uid + 1,
            "SUID": suid,
            "NAME": players_write,
            "CHARACTER": characters_write,
            "MAP": mk_map,
            "DATE": datetime.now().strftime("%Y-%m-%d"),
            "PLAYERS": len(players_write),
            "PLACE": range(1, (1 + len(players_write)), 1),
            "SEASON": last_season,
        }
    )

    data_out = new_data[
        [
            "UID",
            "SUID",
            "NAME",
            "CHARACTER",
            "MAP",
            "PLACE",
            "PLAYERS",
            "DATE",
            "SEASON",
        ]
    ].copy()

    st.write("Does New Input Look Correct?")

    st.dataframe(data=data_out)

    st.write("How About Compared to the Previous Game's Data?")

    concat_df = pd.concat(objs=[df, data_out]).reset_index(drop=True)
    st.write(concat_df.tail(8))

    st.write(
        "IF THE DATA IS NOT CORRECT THEN RELOAD AND TRY AGAIN OR YOU ARE MAJORLY REGARDED"
    )

    commit_data = st.form_submit_button("Commit Correct Data")

    if commit_data:
        dl = data_out.values.tolist()
        ws.append_rows(values=dl)
