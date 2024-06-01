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
            by=[season_column, npi_column, players_column],
            ascending=[True, True, False],
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
            f"2NDS_{percent_column_suffix}",
            f"3RDS_{percent_column_suffix}",
            f"4THS_{percent_column_suffix}",
        ]
    ]

    return all_seasons_stats_df
