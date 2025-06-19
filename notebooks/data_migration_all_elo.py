# %%
import pandas as pd
from multielo import MultiElo, Tracker


# %%
form_data = pd.read_csv(
    r"C:\Users\Cooper\sandbox\mkstream\form_data_migration\form_data_valid.csv"
)


# %%
def get_elo(place_input_df: pd.DataFrame):

    exp_elo = MultiElo(score_function_base=2)

    tracker = Tracker(elo_rater=exp_elo)

    tracker.process_data(place_input_df)

    track_df = tracker.get_history_df()

    # Remove 2, 3, 4 player_id artifacts
    # Sort by date
    track_out = (
        track_df[track_df["player_id"].str.len() > 1]
        .sort_values(by="date", ascending=True)
        .copy()
        .reset_index(drop=True)
    )

    return track_out


# %%


def get_categorical_elo(
    form_data_df: pd.DataFrame, category_column: str
) -> dict[str, pd.DataFrame]:

    four_holder = []
    three_holder = []
    two_holder = []

    for value in form_data_df[category_column].unique():

        four_player = (
            form_data_df[
                (form_data_df["PLAYERS"] == 4)
                & (form_data_df[category_column] == value)
            ]
            .copy()
            .reset_index(drop=True)
            .drop(columns=[category_column])
        )
        three_player = (
            form_data_df[
                (form_data_df["PLAYERS"] == 3)
                & (form_data_df[category_column] == value)
            ]
            .copy()
            .reset_index(drop=True)
            .dropna(axis=1, how="all")
            .drop(columns=[category_column])
        )
        two_player = (
            form_data_df[
                (form_data_df["PLAYERS"] == 2)
                & (form_data_df[category_column] == value)
            ]
            .copy()
            .reset_index(drop=True)
            .dropna(axis=1, how="all")
            .drop(columns=[category_column])
        )

        four = get_elo(four_player)
        three = get_elo(three_player)
        two = get_elo(two_player)

        four[category_column] = value
        three[category_column] = value
        two[category_column] = value

        four_holder.append(four)
        three_holder.append(three)
        two_holder.append(two)

    four_cat = pd.concat(four_holder).reset_index(drop=True)
    three_cat = pd.concat(three_holder).reset_index(drop=True)
    two_cat = pd.concat(two_holder).reset_index(drop=True)

    return {"four_player": four_cat, "three_player": three_cat, "two_player": two_cat}


# %%

form_data_rename = (
    form_data[
        [
            "PLAYERS_1ST",
            "PLAYERS_2ND",
            "PLAYERS_3RD",
            "PLAYERS_4TH",
            "TIMESTAMP",
            "PLAYERS",
            "SEASON",
        ]
    ]
    .copy()
    .rename(
        columns={
            "TIMESTAMP": "date",
            "PLAYERS_1ST": "1st",
            "PLAYERS_2ND": "2nd",
            "PLAYERS_3RD": "3rd",
            "PLAYERS_4TH": "4th",
            "SEASON": "season",
        }
    )
)


per_season_elo = get_categorical_elo(
    form_data_df=form_data_rename, category_column="season"
)

four_df = per_season_elo["four_player"].copy()
three_df = per_season_elo["three_player"].copy()
two_df = per_season_elo["two_player"].copy()

four_df["players"] = 4
three_df["players"] = 3
two_df["players"] = 2

per_season_out = (
    pd.concat([four_df, three_df, two_df])
    .sort_values(by="date", ascending=True)
    .reset_index(drop=True)
)

per_season_out.to_csv(
    rf"C:\Users\Cooper\sandbox\mkstream\form_data_migration\elo_per_season.csv",
    index=False,
)


# %%
form_data_rename_map = (
    form_data[
        [
            "PLAYERS_1ST",
            "PLAYERS_2ND",
            "PLAYERS_3RD",
            "PLAYERS_4TH",
            "TIMESTAMP",
            "PLAYERS",
            "MAP",
        ]
    ]
    .copy()
    .rename(
        columns={
            "TIMESTAMP": "date",
            "PLAYERS_1ST": "1st",
            "PLAYERS_2ND": "2nd",
            "PLAYERS_3RD": "3rd",
            "PLAYERS_4TH": "4th",
            "MAP": "map",
        }
    )
)


per_map_elo = get_categorical_elo(
    form_data_df=form_data_rename_map, category_column="map"
)

four_df_map = per_map_elo["four_player"].copy()
three_df_map = per_map_elo["three_player"].copy()
two_df_map = per_map_elo["two_player"].copy()

four_df_map["players"] = 4
three_df_map["players"] = 3
two_df_map["players"] = 2

per_map_out = (
    pd.concat([four_df_map, three_df_map, two_df_map])
    .sort_values(by="date", ascending=True)
    .reset_index(drop=True)
)

per_map_out.to_csv(
    rf"C:\Users\Cooper\sandbox\mkstream\form_data_migration\elo_per_map.csv",
    index=False,
)

# %%
import os
import plotly.express as px

fig_path = rf"C:\Users\{os.environ['USERNAME']}\sandbox\mkstream\assets\fig"

for game_type in per_map_elo.keys():
    tdf = per_map_elo[game_type].copy()

    for map in tdf["map"].unique():

        map_df = tdf[tdf["map"] == map].copy().reset_index()

        f = px.line(
            data_frame=map_df,
            x="date",
            y="rating",
            color="player_id",
            height=800,
            markers=True,
            title=map,
        )

        f.write_html(rf"{fig_path}\maps\elo_{game_type}_{map}.html")

# %%
from bs4 import BeautifulSoup


for type in per_map_elo.keys():

    html_join = []

    files = [f for f in os.listdir(rf"{fig_path}\maps") if f.__contains__(type)]

    for file in files:
        html_join.append(
            BeautifulSoup(
                open(rf"{fig_path}\maps\{file}", encoding="utf-8"),
                features="html.parser",
            )
        )

    with open(
        rf"{fig_path}\all_elo\all_maps_elo_{type}.html",
        "w",
        encoding="utf-8",
    ) as out_file:
        out_file.write(str(html_join))


# %%
# exp_elo = MultiElo(score_function_base=2)
# exp_elo.get_new_ratings([1200, 900, 800, 1400])
# %%
