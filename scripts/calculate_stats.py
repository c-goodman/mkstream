import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from mktools.get_data import load_data_pd
from mktools.stats import calculate_npi, calculate_all_stats

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

# Calculate the total stats values for each player, season and game type
all_stats_df = calculate_all_stats(
    initial_df=npi_df,
    place_mapping_ref=place_dict,
    wins_column=place_dict[1],
    seconds_column=place_dict[2],
    thirds_column=place_dict[3],
    fourths_column=place_dict[4],
)


## TODO: Write to Google Sheet

