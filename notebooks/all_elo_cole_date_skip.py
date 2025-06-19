# %%
import os
import pandas as pd
from dotenv import load_dotenv
from multielo import MultiElo, Tracker
from mktools.get_data import load_data_pd
from mktools.validate_data import validate_bad_uids
import plotly.express as px
from alive_progress import alive_it
from bs4 import BeautifulSoup

# Load Variables from .env file
load_dotenv()

# Load data_main from google sheet
df_initial = load_data_pd(
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

# TODO: THIS IS LAZY AND A TERRIBLE IDEA!!!
# Duplicate uid was caused by a timestamp equality, will be fixed when we move..
# ..to the website lol
# Will likely need to filter this in any future data migrations...
df = df_initial[df_initial["UID"].astype(int) != 10606].copy()

# Convert date to string for... TODO: Update comment
df["DATE"] = pd.to_datetime(df["DATE"]).astype(str)

# Find UIDs that will break ELO calculation
invalid, valid = validate_bad_uids(df=df, return_valid=True)

vdf = valid.copy()

msk = vdf.duplicated(subset=["UID", "NAME"], keep=False)

dupe_names = vdf[msk]

valid_valid = vdf[~vdf["UID"].isin(dupe_names["UID"])].copy().reset_index(drop=True)

place_msk = valid_valid.duplicated(subset=["UID", "PLACE"], keep=False)

dupe_places = valid_valid[place_msk]

invalid_places = valid_valid[valid_valid["UID"].isin(dupe_places["UID"])]

valid_valid_valid = (
    valid_valid[~valid_valid["UID"].isin(invalid_places["UID"])]
    .copy()
    .reset_index(drop=True)
)

v_invalid, v_valid = validate_bad_uids(df=valid_valid_valid, return_valid=True)

# %%

g = v_valid.groupby("UID").agg(DATE=pd.NamedAgg("DATE", "first")).reset_index()

g["DATE"] = pd.to_datetime(g["DATE"])

g["DELTA"] = g["DATE"].diff()

g = g.sort_values(by="DELTA", ascending=False)

# %%

# import datetime

# v = v_valid.copy()
# v["DATE"] = pd.to_datetime(v["DATE"])

# q = v[(v["SEASON"] == 19) & (v["DATE"] < datetime.datetime(2025, 5, 25))]


# tdf = (
#     v_valid.drop(columns=["SUID", "CHARACTER", "PLAYERS", "SEASON", "MAP"])
#     .sort_values(by=["UID", "DATE", "PLACE"])
#     .copy()
#     .reset_index(drop=True)
# )


# pdf = (
#     tdf.pivot(index="UID", columns="PLACE", values="NAME")
#     .reset_index()
#     .rename(columns={1: "1st", 2: "2nd", 3: "3rd", 4: "4th"})
#     .merge(tdf[["UID", "DATE"]], on="UID", how="inner", validate="1:m")
#     .groupby("UID")
#     .first()
#     # .set_index("DATE")
#     # .reset_index()
#     .rename(columns={"DATE": "date"})
# )


# exp_elo = MultiElo(score_function_base=2)

# tracker = Tracker(elo_rater=exp_elo)

# tracker.process_data(pdf)


# rdf = tracker.get_current_ratings()

# track_df = tracker.get_history_df()

# track_df = track_df[track_df["player_id"].isin(track_df["player_id"])].reset_index(
#     drop=True
# )


# all_fig = px.line(
#     data_frame=track_df,
#     x="date",
#     y="rating",
#     color="player_id",
#     height=800,
#     markers=True,
# )

# all_fig.write_html(
#     rf"C:\Users\{os.environ['USERNAME']}\sandbox\mkstream\assets\fig\all_elo\all_elo.html"
# )


# seasons_tracked = []
# seasons_overall = []

# unique_seasons = v_valid["SEASON"].unique()


# for season in alive_it(
#     it=unique_seasons,
#     total=unique_seasons.shape[0],
#     theme="classic",
# ):

#     for game_type in v_valid[v_valid["SEASON"] == season]["PLAYERS"].unique():

#         tdf = (
#             v_valid[(v_valid["PLAYERS"] == game_type) & (v_valid["SEASON"] == season)]
#             .drop(columns=["SUID", "CHARACTER", "PLAYERS", "SEASON", "MAP"])
#             .sort_values(by=["UID", "PLACE"])
#             .copy()
#             .reset_index(drop=True)
#         )

#         if game_type == 4:

#             pdf = (
#                 tdf.pivot(index="UID", columns="PLACE", values="NAME")
#                 .reset_index()
#                 .rename(columns={1: "1st", 2: "2nd", 3: "3rd", 4: "4th"})
#                 .merge(tdf[["UID", "DATE"]], on="UID", how="inner", validate="1:m")
#                 .groupby("UID")
#                 .first()
#                 .set_index("DATE")
#                 .reset_index()
#                 .rename(columns={"DATE": "date"})
#             )

#         elif game_type == 3:

#             pdf = (
#                 tdf.pivot(index="UID", columns="PLACE", values="NAME")
#                 .reset_index()
#                 .rename(columns={1: "1st", 2: "2nd", 3: "3rd"})
#                 .merge(tdf[["UID", "DATE"]], on="UID", how="inner", validate="1:m")
#                 .groupby("UID")
#                 .first()
#                 .set_index("DATE")
#                 .reset_index()
#                 .rename(columns={"DATE": "date"})
#             )

#         elif game_type == 2:

#             pdf = (
#                 tdf.pivot(index="UID", columns="PLACE", values="NAME")
#                 .reset_index()
#                 .rename(columns={1: "1st", 2: "2nd"})
#                 .merge(tdf[["UID", "DATE"]], on="UID", how="inner", validate="1:m")
#                 .groupby("UID")
#                 .first()
#                 .set_index("DATE")
#                 .reset_index()
#                 .rename(columns={"DATE": "date"})
#             )

#         exp_elo = MultiElo(score_function_base=2)
#         tracker = Tracker(elo_rater=exp_elo)
#         tracker.process_data(pdf)
#         rdf = tracker.get_current_ratings()
#         tr_df = tracker.get_history_df()

#         rdf["SEASON"] = season

#         t_prefix = f"S{season}_P{game_type}"

#         seasons_tracked.append((t_prefix, tr_df))
#         seasons_overall.append((t_prefix, rdf))


# st_dict = dict(seasons_tracked)
# overall_dict = dict(seasons_overall)

# for k in st_dict.keys():

#     temp_df = st_dict[k]

#     f = px.line(
#         data_frame=temp_df,
#         x="date",
#         y="rating",
#         color="player_id",
#         height=800,
#         markers=True,
#         title=k,
#     )

#     f.write_html(
#         rf"C:\Users\{os.environ['USERNAME']}\sandbox\mkstream\assets\fig\elo_{k}.html"
#     )

# file_path = rf"C:\Users\{os.environ['USERNAME']}\sandbox\mkstream\assets\fig"

# html_file_names = pd.Series(
#     [x for x in os.listdir(file_path) if not x.__contains__("all_elo")]
# ).sort_values()

# season_count = [
#     int(x.removesuffix(".html").split("_")[1].replace("S", "")) for x in html_file_names
# ]
# players_count = [
#     int(x.removesuffix(".html").split("_")[2].replace("P", "")) for x in html_file_names
# ]

# files_df = pd.DataFrame(
#     {"FILE_NAME": html_file_names, "SEASON": season_count, "PLAYERS": players_count}
# ).sort_values(["SEASON", "PLAYERS"])

# four_p_df = files_df[files_df["PLAYERS"] == 4].reset_index(drop=True)
# three_p_df = files_df[files_df["PLAYERS"] == 3].reset_index(drop=True)
# two_p_df = files_df[files_df["PLAYERS"] == 2].reset_index(drop=True)

# for fdf in [four_p_df, three_p_df, two_p_df]:

#     html_join = []

#     for file in fdf["FILE_NAME"]:
#         html_join.append(
#             BeautifulSoup(
#                 open(rf"{file_path}\{file}", encoding="utf-8"), features="html.parser"
#             )
#         )

#     with open(
#         rf"{file_path}\all_elo\{fdf['PLAYERS'].max()}P_all_elo.html",
#         "w",
#         encoding="utf-8",
#     ) as out_file:
#         out_file.write(str(html_join))
