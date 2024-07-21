import numpy as np
import pandas as pd


def form_data_wide_to_long(
    form_df: pd.DataFrame,
    mk_data_df: pd.DataFrame,
    form_data_transform_df: pd.DataFrame,
) -> pd.DataFrame:

    ft_df = form_data_transform_df.copy()

    fdf = form_df.copy().rename(columns={"Timestamp": "DATE"})

    fdf["DATE"] = pd.to_datetime(fdf["DATE"]).astype(str)

    fdf = fdf[~fdf["DATE"].isin(ft_df["DATE"])].copy().reset_index(drop=True)

    mdf = (
        mk_data_df.copy()
        .sort_values(by=["UID", "SEASON", "PLACE"], ascending=[True, True, True])
        .reset_index(drop=True)
    )

    last_uid = mdf["UID"].tail(1).values[0]
    last_suid = mdf["SUID"].tail(1).values[0]
    current_season = mdf["SEASON"].tail(1).values[0]

    current_uid = last_uid + 1

    last_season = current_season - 1
    last_uid_of_last_season = mdf[mdf["SEASON"] == last_season]["UID"].tail(1).values[0]

    season_index = abs(current_uid - last_uid_of_last_season)

    games_left_in_season = 550 - (season_index + fdf.shape[0])

    print(f"Season {current_season} Games Remaining: {games_left_in_season}")

    if games_left_in_season < 0:
        current_season = current_season + 1
        print(f"New Season Started: {current_season}")

    # TODO: Improve conditional logic for overflow of season cutoff
    fdf["SEASON"] = current_season

    l = []

    for s in fdf["NEW_SESSION"]:
        if s == "NO":
            l.append(last_suid)
        if s == "YES":
            last_suid += 1
            l.append(last_suid)

    fdf["UID"] = range(current_uid, (current_uid + fdf.shape[0]), 1)

    fdf["SUID"] = l

    wide_df_list = []

    for game_type in fdf["PLAYERS"].unique():

        tdf = fdf[fdf["PLAYERS"] == game_type][
            [
                "UID",
                "SUID",
                "DATE",
                "NEW_SESSION",
                "MAP",
                "PLAYERS",
                *[x for x in fdf.columns if x.__contains__(f"_{game_type}")],
            ]
        ].rename(
            columns={
                f"PLAYERS_{game_type} [1ST]": "NAME_1",
                f"PLAYERS_{game_type} [2ND]": "NAME_2",
                f"PLAYERS_{game_type} [3RD]": "NAME_3",
                f"PLAYERS_{game_type} [4TH]": "NAME_4",
                f"CHARACTERS_{game_type} [1ST]": "CHARACTER_1",
                f"CHARACTERS_{game_type} [2ND]": "CHARACTER_2",
                f"CHARACTERS_{game_type} [3RD]": "CHARACTER_3",
                f"CHARACTERS_{game_type} [4TH]": "CHARACTER_4",
            },
            errors="ignore",
        )

        wdf = pd.wide_to_long(
            df=tdf,
            stubnames=["CHARACTER", "NAME"],
            i=["UID", "SUID", "DATE", "MAP"],
            j="PLACE",
            sep="_",
        ).reset_index()

        wide_df_list.append(wdf)

    wide_df = (
        pd.concat(wide_df_list)
        .sort_values(by=["UID", "PLACE"], ascending=[True, True])
        .reset_index(drop=True)
    )

    # Explicit set of season value may break at start of next season
    # TODO: Improve conditional logic for overflow of season cutoff
    wide_df["SEASON"] = current_season

    wide_sorted = wide_df[
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
    ]

    wide_concat = pd.concat([ft_df, wide_sorted]).sort_values(
        by=["UID", "DATE", "SUID", "SEASON", "PLACE"]
    )

    wide_concat["DATE"] = wide_concat["DATE"].astype(str).str.replace("'", "")

    return wide_concat
