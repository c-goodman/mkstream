import gspread as gs


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
