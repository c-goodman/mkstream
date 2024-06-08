import warnings
import gspread as gs
import pandas as pd
import os

warnings.filterwarnings(
    "ignore",
    message="[Deprecated][in version 6.0.0]: Method signature's arguments",
)


def create_gs_worksheet_connection(sheet_name: str, sheet_id: str) -> gs.Worksheet:
    """Create an edit session for specified Google Sheet. New records can be appended with the
    usage of `gspread.worksheet.Worksheet.append_rows()`.

    Args:
        sheet_name (str): Name of the table to be loaded.
        sheet_id (str): Google Drive Sheets ID Unique value stored in `.env` file.

    Returns:
        gs.Worksheet: Editable Google Sheet object.
    """

    # Initialize connection to Google Sheets Service Account
    gc = gs.service_account()

    # Construct the Google Drive URL using `sheet_id` and `sheet_name`
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    # Load specified Google Sheet based on constructed URL
    sh = gc.open_by_url(url)

    # Create an editable object from the loaded sheet
    # Records can be appended to source Google Sheet
    ws = sh.worksheet(sheet_name)

    return ws


def write_df_to_worksheet(
    df: pd.DataFrame,
    sheet_name: str,
    range_col_start: str = "A1",
    range_col_finish: str = "Z",
):

    idf = df.copy()

    ## all_stats
    # Get the number of records of the array for the gspread range
    records_range = idf.shape[0] + 1

    # Create connection to the output Google Sheet
    ws = create_gs_worksheet_connection(
        sheet_name=sheet_name,
        sheet_id=os.environ["SHEET_ID"],
    )

    # Drop all previous data from the worksheet
    ws.clear()

    # Write the data to the google sheet
    ws.update(
        values=([idf.columns.values.tolist()] + idf.values.tolist()),
        range_name=f"{range_col_start}:{range_col_finish}{records_range}",
    )
