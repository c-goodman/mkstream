import pandas as pd


def validate_bad_uids(df: pd.DataFrame) -> None:

    msk = (df["UID"].value_counts().reset_index()["count"] > 4) | (
        df["UID"].value_counts().reset_index()["count"] < 2
    )

    bad_uids = df[df["UID"].isin(df["UID"].value_counts().index[msk])].reset_index(
        drop=True
    )

    return bad_uids
