import os
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from multielo import MultiElo, Player, Tracker
from mktools.get_data import load_data_pd
from mktools.validate_data import validate_bad_uids
import plotly.express as px

load_dotenv()


df = load_data_pd(
    sheet_name="data_main",
    sheet_id=os.environ["SHEET_ID"],
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
)

df["DATE"] = pd.to_datetime(df["DATE"]).astype(str)

invalid, valid = validate_bad_uids(df=df, return_valid=True)

tdf = (
    valid[(valid["PLAYERS"] == 4) & (valid["SEASON"] == 11)]
    .drop(columns=["SUID", "CHARACTER", "PLAYERS", "SEASON", "MAP"])
    .sort_values(by=["UID", "PLACE"])
    .copy()
    .reset_index(drop=True)
)

pdf = (
    tdf.pivot(index="UID", columns="PLACE", values="NAME")
    .reset_index()
    .rename(columns={1: "1st", 2: "2nd", 3: "3rd", 4: "4th"})
    .merge(tdf[["UID", "DATE"]], on="UID", how="inner", validate="1:m")
    .groupby("UID")
    .first()
    .set_index("DATE")
    .reset_index()
    .rename(columns={"DATE": "date"})
)

exp_elo = MultiElo(score_function_base=2)
tracker = Tracker(elo_rater=exp_elo)
tracker.process_data(pdf)

rdf = tracker.get_current_ratings()

filter_df = rdf.copy()

track_df = tracker.get_history_df()

track_df = track_df[track_df["player_id"].isin(filter_df["player_id"])].reset_index(
    drop=True
)


fig = px.line(
    data_frame=track_df,
    x="date",
    y="rating",
    color="player_id",
    height=800,
    markers=True,
)

fig.write_html(r"C:\Users\Cooper\sandbox\mkstream\assets\fig\s11_4p_elo.html")
