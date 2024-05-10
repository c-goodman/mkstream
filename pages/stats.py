import streamlit as st
import pandas as pd
import polars as pl
import numpy as np
from plotly import express as px
from typing import List
from home import load_data

st.markdown("# Stats")


df = load_data().to_pandas()


def season_frames(df: pd.DataFrame, season: int):
    """Return a list of DataFrames specific to a single season where
    each DataFrame contains only rows related to PLAYERS column values.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        season (int): Season Number

    Returns:
        List: List of DataFrames
    """
    # Create DataFrame of rows belonging to the specified season number
    season_df = df.loc[df.SEASON == season]

    # Create DataFrames where rows are related to the Players column
    two_p = season_df.loc[season_df.PLAYERS == 2]
    three_p = season_df.loc[season_df.PLAYERS == 3]
    four_p = season_df.loc[season_df.PLAYERS == 4]

    return [two_p, three_p, four_p]


def basic_ops(sdf: pd.DataFrame):
    """Perform basic transformations shared by the two_player(),
    three_player(), and four_player() functions. Create 'GAMES_PLAYED',
    'WINS', and 'WIN_PERCENTAGE' columns.

    Args:
        sdf (pd.DataFrame): Input DataFrame of Season and Player Type

    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    # Create new empty DataFrame and Column for Unique NAME values
    df = pd.DataFrame()
    df["NAME"] = sdf["NAME"].unique()

    # Group the old DataFrame together by NAME and count frequency
    gby = sdf[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()
    gby = gby.reset_index()  # Reset the Index to push out NAME column

    # Merge on NAME value, rename groupby column to GAMES_PLAYED
    df = pd.merge(df, gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "GAMES_PLAYED"})
    df = df.fillna(0)

    # Create DataFrame of only Wins, groupby NAME and count frequency
    first = sdf.loc[sdf.PLACE == 1]
    f_gby = first[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()

    # Merge on NAME value, rename groupby column to WINS, fill nan with 0s
    df = pd.merge(df, f_gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "WINS"})
    df = df.fillna(0)
    df["WINS"] = df["WINS"].astype(int)

    df["WIN_PERCENTAGE"] = (df["WINS"] / df["GAMES_PLAYED"]) * 100
    return df


def npi_gatekeeper(df, sdf, players: int, percent: float):
    all_games_played = sdf.shape[0] / players

    threshold = all_games_played * percent

    yuh_fam = df.loc[df["GAMES_PLAYED"] > threshold]
    nah_fam = df.loc[df["GAMES_PLAYED"] < threshold]

    return yuh_fam, nah_fam, threshold


def two_player(sdf):
    df = basic_ops(sdf=sdf)
    df["2NDS"] = df["GAMES_PLAYED"] - df["WINS"]
    df["2NDS_PERCENTAGE"] = (df["2NDS"] / df["GAMES_PLAYED"]) * 100

    df["NPI"] = ((1 * df["WINS"]) + (2 * df["2NDS"])) / df["GAMES_PLAYED"]

    df = df[
        [
            "NAME",
            "NPI",
            "GAMES_PLAYED",
            "WINS",
            "WIN_PERCENTAGE",
            "2NDS",
            "2NDS_PERCENTAGE",
        ]
    ]

    df, nah, two_p_threshold = npi_gatekeeper(df=df, sdf=sdf, players=2, percent=0.1)

    df = df.sort_values(by="NPI")
    two_p_df = df.reset_index(drop=True)

    nah = nah.sort_values(by="NPI")
    two_p_nah = nah.reset_index(drop=True)

    return two_p_df, two_p_nah, two_p_threshold


def three_player(sdf):
    df = basic_ops(sdf=sdf)

    second = sdf.loc[sdf.PLACE == 2]
    s_gby = second[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()

    df = pd.merge(df, s_gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "2NDS"})
    df = df.fillna(0)
    df["2NDS"] = df["2NDS"].astype(int)

    df["2NDS_PERCENTAGE"] = (df["2NDS"] / df["GAMES_PLAYED"]) * 100

    df["3RDS"] = df["GAMES_PLAYED"] - (df["WINS"] + df["2NDS"])
    df["3RDS_PERCENTAGE"] = (df["3RDS"] / df["GAMES_PLAYED"]) * 100

    df["NPI"] = ((1 * df["WINS"]) + (2 * df["2NDS"]) + (3 * df["3RDS"])) / df[
        "GAMES_PLAYED"
    ]

    df = df[
        [
            "NAME",
            "NPI",
            "GAMES_PLAYED",
            "WINS",
            "WIN_PERCENTAGE",
            "2NDS",
            "2NDS_PERCENTAGE",
            "3RDS",
            "3RDS_PERCENTAGE",
        ]
    ]

    df, nah, three_p_threshold = npi_gatekeeper(df=df, sdf=sdf, players=3, percent=0.1)

    df = df.sort_values(by="NPI")
    three_p_df = df.reset_index(drop=True)

    nah = nah.sort_values(by="NPI")
    three_p_nah = nah.reset_index(drop=True)

    return three_p_df, three_p_nah, three_p_threshold


def four_player(sdf):
    df = pd.DataFrame()
    df["NAME"] = sdf["NAME"].unique()

    gby = sdf[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()
    gby = gby.reset_index()

    df = pd.merge(df, gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "GAMES_PLAYED"})
    df = df.fillna(0)

    first = sdf.loc[sdf.PLACE == 1]
    f_gby = first[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()

    df = pd.merge(df, f_gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "WINS"})
    df = df.fillna(0)
    df["WINS"] = df["WINS"].astype(int)

    df["WIN_PERCENTAGE"] = (df["WINS"] / df["GAMES_PLAYED"]) * 100

    second = sdf.loc[sdf.PLACE == 2]
    s_gby = second[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()

    df = pd.merge(df, s_gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "2NDS"})
    df = df.fillna(0)
    df["2NDS"] = df["2NDS"].astype(int)

    df["2NDS_PERCENTAGE"] = (df["2NDS"] / df["GAMES_PLAYED"]) * 100

    third = sdf.loc[sdf.PLACE == 3]
    t_gby = third[["NAME", "UID"]].groupby(by="NAME", dropna=False).count()

    df = pd.merge(df, t_gby, on="NAME", how="outer")
    df = df.rename(columns={"UID": "3RDS"})
    df = df.fillna(0)
    df["3RDS"] = df["3RDS"].astype(int)

    df["3RDS_PERCENTAGE"] = (df["3RDS"] / df["GAMES_PLAYED"]) * 100

    df["4THS"] = df["GAMES_PLAYED"] - (df["WINS"] + df["2NDS"] + df["3RDS"])
    df["4THS_PERCENTAGE"] = (df["4THS"] / df["GAMES_PLAYED"]) * 100

    df["NPI"] = (
        (1 * df["WINS"]) + (2 * df["2NDS"]) + (3 * df["3RDS"]) + (4 * df["4THS"])
    ) / df["GAMES_PLAYED"]

    df = df[
        [
            "NAME",
            "NPI",
            "GAMES_PLAYED",
            "WINS",
            "WIN_PERCENTAGE",
            "2NDS",
            "2NDS_PERCENTAGE",
            "3RDS",
            "3RDS_PERCENTAGE",
            "4THS",
            "4THS_PERCENTAGE",
        ]
    ]
    
    df, nah, four_p_threshold = npi_gatekeeper(df=df, sdf=sdf, players=3, percent=0.1)

    df = df.sort_values(by="NPI")
    four_p_df = df.reset_index(drop=True)

    nah = nah.sort_values(by="NPI")
    four_p_nah = nah.reset_index(drop=True)

    return four_p_df, four_p_nah, four_p_threshold


def season_iterator(df, season_list: List[int]):

    stats_list = []
    nah_list = []
    threshold_list = []
    s_list = []

    for i in season_list:
        season_i = season_frames(df=df, season=i)

        # Call Stats Functions on the list of season_frames
        i_4p_stats, i_4p_nah, i_4p_threshold = four_player(sdf=season_i[2])
        i_3p_stats, i_3p_nah, i_3p_threshold = three_player(sdf=season_i[1])
        i_2p_stats, i_2p_nah, i_2p_threshold = two_player(sdf=season_i[0])

        # Append the Stats DataFrames to the list
        stats_list.append(i_4p_stats)
        stats_list.append(i_3p_stats)
        stats_list.append(i_2p_stats)

        # Append str to s_list
        s_list.append("Four Player")
        s_list.append("Three Player")
        s_list.append("Two Player")

        # Append nah's to list
        nah_list.append(i_4p_nah)
        nah_list.append(i_3p_nah)
        nah_list.append(i_2p_nah)

        # Append thresholds to list
        threshold_list.append(i_4p_threshold)
        threshold_list.append(i_3p_threshold)
        threshold_list.append(i_2p_threshold)

        stats_df = pd.DataFrame()
        stats_df["stats_list"] = stats_list
        stats_df["s_list"] = s_list
        stats_df["nah_list"] = nah_list
        stats_df["threshold_list"] = threshold_list

    return stats_df


sl = season_iterator(df=df, season_list=[6, 5, 4, 3, 2, 1, 0])

for idx, i in enumerate(sl.stats_list):
    if idx == 0:
        st.markdown("## Season 6")
    elif idx == 3:
        st.markdown("## Season 5")
    elif idx == 6:
        st.markdown("## Season 4")
    elif idx == 9:
        st.markdown("## Season 3")
    elif idx == 12:
        st.markdown("## Season 2")
    elif idx == 15:
        st.markdown("## Season 1")
    elif idx == 18:
        st.markdown("## Season 0")

    st.markdown(f"### {sl.s_list[idx]}")
    st.write(i)
    i_npi = px.histogram(i, x="NAME", y="NPI")
    st.plotly_chart(i_npi)
    st.write(f"GAMES PLAYED CUTOFF THRESHOLD: {sl.threshold_list[idx]}")
    st.write(sl.nah_list[idx])
    idx_npi = px.histogram(sl.nah_list[idx], x="NAME", y="NPI")
    st.plotly_chart(idx_npi)
    