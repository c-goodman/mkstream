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

names_unique = df["NAME"].unique().to_list()
map_names_unique = df["MAP"].unique().to_list()
char_names_unique = df["CHARACTER"].unique().to_list()

last_suid = df.select(pl.last("SUID")).item()
last_uid = df.select(pl.last("UID")).item()


# last_season = int(df.select(pl.last("SEASON")).item())

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

        new_data = pl.DataFrame(
            data={
                "UID": last_uid + 1,
                "SUID": suid,
                "NAME": players_write,
                "CHARACTER": characters_write,
                "MAP": mk_map,
                "DATE": datetime.now().strftime("%Y-%m-%d"),
                "PLAYERS": len(players_write),
                "PLACE": range(1, (1 + len(players_write)), 1),
            }
        )

        # new_data = new_data.with_columns(
        #     PLACE=pl.int_range(start=1, end=(1 + new_data.shape[0]), step=1)
        # )

        # new_data = new_data.with_columns(PLAYERS=new_data.shape[0])
        # new_data = new_data.with_columns(pl.col("PLAYERS").cast(dtype=pl.Int32))

        out_data = new_data[
            ["UID", "SUID", "NAME", "CHARACTER", "MAP", "PLACE", "PLAYERS", "DATE"]
        ]

        st.write(out_data)

        concat_df = pl.concat(items=[df, out_data], how="vertical_relaxed")

        st.write(concat_df.tail(8))

        some_name_not_taken = out_data.to_pandas().values.tolist()
        ws.append_rows(some_name_not_taken)
