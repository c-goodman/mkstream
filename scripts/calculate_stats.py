import os
import warnings
import numpy as np
import pandas as pd
from dotenv import load_dotenv

warnings.filterwarnings(
    "ignore",
    message="Method signature's arguments 'range_name' and 'values' will change their order.",
)

from mktools.get_data import load_data_pd
from mktools.stats import (
    calculate_current_suid_character_wins,
    calculate_npi,
    calculate_all_stats,
    calculate_all_win_rates,
    calculate_octets,
    sort_popular,
)
from mktools.write_data import write_df_to_worksheet

load_dotenv()

# Load Data from the Google Sheet
df = load_data_pd(
    sheet_name="data",
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

# Calculate the overall NPI number for each player per season per game type
npi_df = calculate_npi(initial_df=df)

# Define the column name mappings for each possible place
place_dict = {1: "WINS", 2: "2NDS", 3: "3RDS", 4: "4THS"}

## all_stats
# Calculate the total stats values for each player, season and game type
print("-" * 30)
print("All Stats")
print("-" * 30)
print("\n")

all_stats_df = calculate_all_stats(
    initial_df=npi_df,
    place_mapping_ref=place_dict,
    wins_column=place_dict[1],
    seconds_column=place_dict[2],
    thirds_column=place_dict[3],
    fourths_column=place_dict[4],
)

write_df_to_worksheet(df=all_stats_df, sheet_name="all_stats")

## all_wr
print("-" * 30)
print("All Win Rate")
print("-" * 30)
print("\n")

all_wr_df = calculate_all_win_rates(
    stats_df=all_stats_df,
    wins_column=place_dict[1],
    seconds_column=place_dict[2],
    thirds_column=place_dict[3],
    fourths_column=place_dict[4],
)

write_df_to_worksheet(df=all_wr_df, sheet_name="all_win_rate")

## octets
print("-" * 30)
print("Octets")
print("-" * 30)
print("\n")

octets_df = calculate_octets(df=df)

write_df_to_worksheet(df=octets_df, sheet_name="octets")

## Current Session Wins per Player
print("-" * 30)
print("Current Session Wins")
print("-" * 30)
print("\n")

current_wins = calculate_current_suid_character_wins(df=df)

write_df_to_worksheet(df=current_wins, sheet_name="current_suid_wins")


## Sorted Name, Character and Map for Dropdowns
print("-" * 30)
print("Dropdown Values")
print("-" * 30)
print("\n")

# Get sorted names
popular_names = sort_popular(df=df, col="NAME").drop(
    columns=["all_count", "current_count"]
)

# Get sorted characters
popular_characters = sort_popular(df=df, col="CHARACTER").drop(
    columns=["all_count", "current_count"]
)

# Get sorted maps
popular_maps = sort_popular(df=df, col="MAP").drop(
    columns=["all_count", "current_count"]
)

# Write dropdown data to sheets
write_df_to_worksheet(
    df=popular_names,
    sheet_name="name_dd",
    range_col_start="A1",
    range_col_finish=f"A",
    headers=False,
)

write_df_to_worksheet(
    df=popular_characters,
    sheet_name="character_dd",
    range_col_start="A1",
    range_col_finish=f"A",
    headers=False,
)

write_df_to_worksheet(
    df=popular_maps,
    sheet_name="map_dd",
    range_col_start="A1",
    range_col_finish=f"A",
    headers=False,
)
