import os
from dotenv import load_dotenv

from mktools.get_data import load_data_pd
from mktools.stats import calculate_npi, calculate_all_stats, calculate_all_win_rates
from mktools.write_data import create_gs_worksheet_connection

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


## all_stats
# Get the number of records of the array for the gspread range
records_range = all_stats_df.shape[0] + 1

# Create connection to the output Google Sheet
ws = create_gs_worksheet_connection(
    sheet_name="all_stats",
    sheet_id=os.environ["SHEET_ID"],
)

# Drop all previous data from the worksheet
ws.clear()

# Write the data to the google sheet
ws.update(
    values=([all_stats_df.columns.values.tolist()] + all_stats_df.values.tolist()),
    range_name=f"A1:M{records_range}",
)

## all_wr
all_wr_df = calculate_all_win_rates(
    stats_df=all_stats_df,
    wins_column=place_dict[1],
    seconds_column=place_dict[2],
    thirds_column=place_dict[3],
    fourths_column=place_dict[4],
)


## all_win_rate
# Get the number of records of the array for the gspread range
records_range = all_wr_df.shape[0] + 1

# Create connection to the output Google Sheet
ws_wr = create_gs_worksheet_connection(
    sheet_name="all_win_rate",
    sheet_id=os.environ["SHEET_ID"],
)

# Drop all previous data from the worksheet
ws_wr.clear()

# Write the data to the google sheet
ws_wr.update(
    values=([all_wr_df.columns.values.tolist()] + all_wr_df.values.tolist()),
    range_name=f"A1:M{records_range}",
)
