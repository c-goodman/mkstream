import os
import time
import warnings
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from mktools.get_data import load_data_pd
from mktools.stats import (
    calculate_current_suid_character_wins,
    calculate_npi,
    calculate_all_stats,
    calculate_all_win_rates,
    calculate_octets,
    sort_popular,
    calculate_my_maps,
)
from mktools.validate_data import validate_bad_uids
from mktools.write_data import write_df_to_worksheet
from mktools.form_data import form_data_wide_to_long

warnings.filterwarnings(
    "ignore",
    message="Method signature's arguments 'range_name' and 'values' will change their order.",
)

load_dotenv()

# Load Data from the Google Sheet
idf = load_data_pd(
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


print("-" * 30)
print("Transform Form Data")
print("-" * 30)
print("\n")

form_df = load_data_pd(sheet_name="form_data", sheet_id=os.environ["SHEET_ID"])

form_df = form_df.drop(
    columns=[x for x in form_df.columns if x.__contains__("Unnamed")]
)

# Get last unique datetime values from data_main sheet and form_data sheet
last_data_main_date = idf["DATE"].tail(1).values[0]
last_form_date = form_df["Timestamp"].tail(1).values[0]

# If there is no new data from the form set initial data_main DataFrame to reused variable..
# ..and do not attempt to transform and append non-existent data
if last_data_main_date == last_form_date:
    print("No New Data Skipping Transform and Append")

    df = idf.copy()

# If there is at least one new record in the form_data sheet transform it to data_main long format
elif last_data_main_date != last_form_date:
    form_transformed_df = load_data_pd(
        sheet_name="form_data_transform", sheet_id=os.environ["SHEET_ID"]
    )

    form_transformed_df = form_transformed_df.drop(
        columns=[x for x in form_transformed_df.columns if x.__contains__("Unnamed")]
    )

    form_transformed_df["DATE"] = pd.to_datetime(form_transformed_df["DATE"]).astype(
        str
    )

    transformed_df = form_data_wide_to_long(
        form_df=form_df,
        mk_data_df=idf,
        form_data_transform_df=form_transformed_df,
        # Enable SUID creation based on rolling 24hr window with cutoff time (currently 7AM UTC)
        enable_time_based_sessions=True,
    )

    write_df_to_worksheet(df=transformed_df, sheet_name="form_data_transform")

    # Wait for IMPORTRANGE to move new transformed data to data_main sheet
    time.sleep(1)

    # Reload Updated Data from the Google Sheet
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

    # Double check that newest data exists in data_main
    if df["UID"].max() == transformed_df["UID"].max():
        pass

    # If it does not exist extend the wait
    else:
        print("Data has not yet updated. (Wait 5s for IMPORTRANGE)")

        # Wait for IMPORTRANGE to move new transformed data to data_main sheet
        time.sleep(10)

        # If data is still not updated raise error
        if not df["UID"].max() == transformed_df["UID"].max():
            raise IndexError("Data still not updated, exiting process.")


# Calculate the overall NPI number for each player per season per game type
npi_df = calculate_npi(initial_df=df)

# Define the column name mappings for each possible place
place_dict = {1: "WINS", 2: "2NDS", 3: "3RDS", 4: "4THS"}

# all_stats
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

# all_wr
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

# octets
print("-" * 30)
print("Octets")
print("-" * 30)
print("\n")

octets_df = calculate_octets(df=df)

write_df_to_worksheet(df=octets_df, sheet_name="octets")

# Current Session Wins per Player
print("-" * 30)
print("Current Session Wins")
print("-" * 30)
print("\n")

current_wins = calculate_current_suid_character_wins(df=df)

write_df_to_worksheet(df=current_wins, sheet_name="current_suid_wins")


# Sorted Name, Character and Map for Dropdowns
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


# my_maps
print("-" * 30)
print("My Maps")
print("-" * 30)
print("\n")

my_maps_df = calculate_my_maps(df=df)

write_df_to_worksheet(df=my_maps_df, sheet_name="all_my_maps")


print("-" * 30)
print("Bad UIDs")

bad_uids_df = validate_bad_uids(df=df)

write_df_to_worksheet(df=bad_uids_df, sheet_name="bad_uids")

print(f"Bad UID Records: {bad_uids_df.shape[0]}")
print(
    f"Percent of Total Records: {np.round((bad_uids_df.shape[0] / df.shape[0]) * 100, 2)}%"
)

shame_df = bad_uids_df[bad_uids_df["PLACE"] == 1]["NAME"].value_counts().reset_index()

print(shame_df)

print("-" * 30)
print("\n")
