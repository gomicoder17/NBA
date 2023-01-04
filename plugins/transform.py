import os
import pandas as pd


def transform_players_info(ti):
    """
    Transforms players info dict from SportsDataIO API to a pandas dataframe.

    Returns: dataframe with players info.
    """
    # ARGS
    players_info_json = ti.xcom_pull(
        task_ids="get_bytesio_players_images", key="players_info_json"
    )

    # FUNCTION
    df = pd.DataFrame(players_info_json)
    df["Name"] = df["FirstName"] + " " + df["LastName"]
    df["Age"] = df["BirthDate"].apply(
        lambda x: pd.Timestamp.today().year - pd.Timestamp(x).year
    )
    columns = [
        "PhotoBytesIO",
        "Name",
        "Age",
        "BirthCountry",
        "Height",
        "Weight",
        "Jersey",
        "Position",
    ]

    return (
        df[columns]
        .sort_values(by=["Name"])
        .reset_index(drop=True)
        .rename(
            columns={
                "BirthCountry": "Birth Country",
            }
        )
    )


def transform_players_stats(ti):
    """
    Transforms players stats dict from SportsDataIO API to a dataframe.

    Returns: dataframe with players stats.
    """
    # ARGS
    players_stats_json = ti.xcom_pull(
        task_ids="get_bytesio_players_images", key="players_stats_json"
    )

    # FUNCTION
    columns = [
        "PhotoBytesIO",
        "Name",
        "Minutes",
        "FreeThrowsMade",
        "FieldGoalsMade",
        "Points",
        "OffensiveRebounds",
        "DefensiveRebounds",
        "Assists",
        "Steals",
        "BlockedShots",
        "Turnovers",
        "PersonalFouls",
    ]

    df = pd.DataFrame(players_stats_json, columns=columns)

    # Parse numeric columns to int
    for col in df.columns:
        if col not in ["Name", "PhotoBytesIO"]:
            df[col] = df[col].round().astype(int)

    return df[columns].rename(
        columns={
            "Minutes": "Mins",
            "FreeThrowsMade": "FT",
            "TwoPointersMade": "2Pt",
            "ThreePointersMade": "3Pt",
            "FieldGoalsMade": "FG",
            "Points": "Pts",
            "OffensiveRebounds": "OReb",
            "DefensiveRebounds": "DReb",
            "Assists": "Ast",
            "Steals": "Stl",
            "BlockedShots": "Blk",
            "Turnovers": "TO",
            "PersonalFouls": "Foul",
            "Efficiency": "Eff",
        }
    )
