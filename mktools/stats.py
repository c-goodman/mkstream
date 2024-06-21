import pandas as pd


def calculate_npi(
    initial_df: pd.DataFrame,
    name_column: str = "NAME",
    place_column: str = "PLACE",
    players_column: str = "PLAYERS",
    season_column: str = "SEASON",
    npi_column: str = "NPI",
) -> pd.DataFrame:

    wdf = initial_df.copy()

    # Initial groupby to get the count of games played per name..
    # ..per game type (players) per place per season
    all_grouped = (
        wdf.groupby([name_column, place_column, players_column, season_column])
        .agg(
            COUNT=pd.NamedAgg(name_column, "count"),
        )
        .reset_index()
        .sort_values(by=[season_column, "COUNT"], ascending=[True, False])
        .reset_index(drop=True)
    )

    # Groupby to get the total number of games played per name..
    # ..per game type (players) per season
    total_games_per_season_per_player = (
        all_grouped.groupby([name_column, season_column, players_column])
        .agg(
            TOTAL_PLAYED=pd.NamedAgg("COUNT", "sum"),
        )
        .reset_index()
    )

    # Merge the two grouped DataFrames together
    total_merge = pd.merge(
        all_grouped,
        total_games_per_season_per_player,
        on=[name_column, season_column, players_column],
        how="inner",
    )

    # Get the initial win percentage for each record
    total_merge["PERCENT"] = (total_merge["COUNT"] / total_merge["TOTAL_PLAYED"]) * 100

    # Get an intermediate Nominal Position Index (NPI) value for each record
    total_merge["NPI_INTERMEDIATE"] = (
        total_merge[place_column] * total_merge["COUNT"]
    ) / total_merge["TOTAL_PLAYED"]

    # Groupby to aggregate the Intermediate NPI values to get total NPI per name..
    # per game type (players) per season
    total_npi = (
        total_merge.groupby([name_column, players_column, season_column])
        .agg(NPI=pd.NamedAgg(column="NPI_INTERMEDIATE", aggfunc="sum"))
        .sort_values(by=[npi_column])
    ).reset_index()

    # Merge in the new total NPI values
    npi_merge = (
        pd.merge(
            total_merge,
            total_npi,
            on=[name_column, season_column, players_column],
            how="inner",
        )
        .sort_values(
            by=[season_column, npi_column, players_column],
            ascending=[True, True, False],
        )
        .reset_index(drop=True)
    )

    return npi_merge


def calculate_all_stats(
    initial_df: pd.DataFrame,
    place_mapping_ref: dict,
    wins_column: str,
    seconds_column: str,
    thirds_column: str,
    fourths_column: str,
    percent_column_suffix: str = "PERCENTAGE",
    name_column: str = "NAME",
    place_column: str = "PLACE",
    players_column: str = "PLAYERS",
    season_column: str = "SEASON",
    npi_column: str = "NPI",
) -> pd.DataFrame:

    wdf = initial_df.copy()

    # Initialize an empty list to hold transformed DataFrames
    df_list = []

    # For each unique value in the place column
    for place in wdf[place_column].unique():

        # Create temporary dataframe filtered by the place value
        temp = wdf[wdf[place_column] == place].copy()

        # Retrieve the mapping value from the reference dictionary
        map_value = place_mapping_ref[place]

        # Rename the count and percent columns using the mapping value
        # Drop the place and intermediate NPI columns
        temp = temp.rename(
            columns={
                "COUNT": map_value,
                "PERCENT": f"{map_value}_{percent_column_suffix}",
            }
        ).drop(columns=[place_column, "NPI_INTERMEDIATE"])

        # Append the transformed dataframe to the list
        df_list.append(temp)

    # Concatenate all the transformed DataFrames
    cat_df = pd.concat(df_list)

    # Group the concatenated DataFrame by name, game type (players) and season
    # Fill Null values in the renamed percentage and count columns with 0..
    # .. to account for (0 games played, 0 win percent) in those game types
    # Sort by Season, NPI and game type
    # Specify the column order for the output
    all_seasons_stats_df = (
        cat_df.groupby([name_column, players_column, season_column])
        .max()
        .fillna(0)
        .reset_index()
        .sort_values(
            by=[season_column, players_column, npi_column],
            ascending=[True, False, True],
        )
        .reset_index(drop=True)
    )[
        [
            npi_column,
            name_column,
            players_column,
            season_column,
            "TOTAL_PLAYED",
            wins_column,
            f"{wins_column}_{percent_column_suffix}",
            seconds_column,
            thirds_column,
            fourths_column,
            f"{seconds_column}_{percent_column_suffix}",
            f"{thirds_column}_{percent_column_suffix}",
            f"{fourths_column}_{percent_column_suffix}",
        ]
    ]

    return all_seasons_stats_df


def calculate_all_win_rates(
    stats_df: pd.DataFrame,
    wins_column: str,
    seconds_column: str,
    thirds_column: str,
    fourths_column: str,
    percent_column_suffix: str = "PERCENTAGE",
    name_column: str = "NAME",
    players_column: str = "PLAYERS",
) -> pd.DataFrame:

    sdf = stats_df.copy()

    all_wr_df = (
        sdf.groupby(by=[name_column, players_column])
        .agg(
            TOTAL_WINS=pd.NamedAgg("WINS", "sum"),
            TOTAL_GAMES_PLAYED=pd.NamedAgg("TOTAL_PLAYED", "sum"),
            OVERALL_WIN_RATE=pd.NamedAgg(
                f"{wins_column}_{percent_column_suffix}", "mean"
            ),
            OVERALL_2NDS=pd.NamedAgg(seconds_column, "sum"),
            OVERALL_3RDS=pd.NamedAgg(thirds_column, "sum"),
            OVERALL_4THS=pd.NamedAgg(fourths_column, "sum"),
            OVERALL_2NDS_RATE=pd.NamedAgg(
                f"{seconds_column}_{percent_column_suffix}", "mean"
            ),
            OVERALL_3RDS_RATE=pd.NamedAgg(
                f"{thirds_column}_{percent_column_suffix}", "mean"
            ),
            OVERALL_4THS_RATE=pd.NamedAgg(
                f"{fourths_column}_{percent_column_suffix}", "mean"
            ),
            NPI=pd.NamedAgg("NPI", "mean"),
        )
        .reset_index()
        .sort_values(
            by=[
                players_column,
                "OVERALL_WIN_RATE",
            ],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )

    return all_wr_df


def calculate_octets(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for octets you dummy.

    Args:
        df (pd.DataFrame): All mkdata input.

    Returns:
        pd.DataFrame: Filtered DataFrame for octets
    """

    idf = df.copy()

    # Create uniqueid for season and session uid combination
    idf["SEASON_SUID"] = [
        f"{idf['SUID'][idx]}_{idf['SEASON'][idx]}" for idx, x in enumerate(idf["SUID"])
    ]

    # Filter for players that got a first place in each season and suid combo
    # Aggregate the count of the games they won in each session
    season_suid_wins_df = (
        idf[idf["PLACE"] == 1]
        .groupby(
            [
                "NAME",
                "SEASON_SUID",
            ]
        )
        .agg(
            COUNT=pd.NamedAgg("NAME", "count"),
            SEASON=pd.NamedAgg("SEASON", "first"),
            SUID=pd.NamedAgg("SUID", "first"),
        )
        .reset_index()
        .sort_values(
            by=["COUNT"],
        )
        .reset_index(drop=True)
        .drop(columns=["SEASON_SUID"])
    )

    # Filter for players that won 8 or more games in one session
    octet_df = (
        season_suid_wins_df[(season_suid_wins_df["COUNT"] >= 8)]
        .copy()
        .sort_values(by=["SEASON", "SUID"])
        .reset_index(drop=True)
    )

    return octet_df


def calculate_current_suid_character_wins(df: pd.DataFrame) -> pd.DataFrame:

    idf = df.copy()

    # Get the number of the current season
    current_suid = idf.tail(1)["SUID"].values[0]

    # Filter all mkdata for current session and season
    current_session_df = (
        idf[(idf["SUID"] == current_suid)].copy().reset_index(drop=True)
    )

    # Filter for first places and simplify DataFrame
    current_session_wins_df = current_session_df[current_session_df["PLACE"] == 1][
        ["NAME", "CHARACTER"]
    ].copy()

    # Get the count of wins per character per player
    # Alter the axis to get long DataFrame where each character is the index
    current_session_wins_per_player_df = (
        pd.crosstab(
            current_session_wins_df["NAME"], current_session_wins_df["CHARACTER"]
        )
        .T.reset_index()
        .rename_axis(None, axis=1)
        .sort_values(by=["CHARACTER"])
    )

    return current_session_wins_per_player_df


def sort_popular(df: pd.DataFrame, col: str):

    wdf = df.copy()

    # Get value counts of entire dataset for a specified column
    all_df = (
        wdf[col].value_counts().reset_index().rename(columns={"count": "all_count"})
    )

    # Get value counts of most recent season for a specified column
    current_df = (
        wdf[wdf["SEASON"] == wdf["SEASON"].max()][col]
        .value_counts()
        .reset_index()
        .rename(columns={"count": "current_count"})
    )

    # Merge current season value counted data with overall
    # Left merge to create null values in unshared current season values
    # Fill null count values with 0 for sorting
    # Sort by current season value counts first descending then all season counts
    merge_df = (
        pd.merge(all_df, current_df, on=col, how="left")
        .fillna(0)
        .sort_values(by=["current_count", "all_count"], ascending=[False, False])
        .reset_index(drop=True)
    )

    return merge_df


def calculate_my_maps(df: pd.DataFrame) -> pd.DataFrame:

    wdf = df.copy()

    wins_gb = (
        wdf[wdf["PLACE"] == 1]
        .groupby(["MAP", "NAME", "PLAYERS"])
        .agg(WINS=pd.NamedAgg("UID", "count"))
        .reset_index()
    )

    all_gb = (
        wdf.groupby(["MAP", "NAME", "PLAYERS"])
        .agg(GAMES_PLAYED=pd.NamedAgg("UID", "count"))
        .reset_index()
    )

    mdf = pd.merge(wins_gb, all_gb, on=["MAP", "NAME", "PLAYERS"])

    mdf["WIN_PERCENTAGE"] = (mdf["WINS"] / mdf["GAMES_PLAYED"]) * 100

    mdf_sorted = mdf.sort_values(
        by=["MAP", "PLAYERS", "WIN_PERCENTAGE"], ascending=[True, False, False]
    ).reset_index(drop=True)

    return mdf_sorted
