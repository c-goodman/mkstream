import numpy as np
import pandas as pd


def load_data_pd(sheet_name: str, sheet_id: str, usecols: list[str]) -> pd.DataFrame:
    """Load specified sheet from Google Drive.

    Args:
        sheet_name (str): Name of the table to be loaded
        sheet_id (str): Google Drive Sheets ID Unique value stored in `.env` file.
        Environment variable is accessible with `dotenv.load_dotenv()` and `os.environ["<YOUR_SHEET_ID>"]`.

    Returns:
        pd.DataFrame: Pandas DataFrame loaded from the specified Google Drive sheet.
    """

    # Construct the Google Drive URL using `sheet_id` and `sheet_name`
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    # Load the data from the URL as a Polars DataFrame
    # Fill Null values with `np.nan`
    df = pd.read_csv(
        url,
        usecols=usecols,
    ).fillna(np.nan)

    return df
