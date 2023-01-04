import json
import os
import requests
from io import BytesIO
from multiprocessing.pool import ThreadPool
import utils
import subprocess

import bs4
import pandas as pd
import html2image

# Overrite html2image ChromeHeadless.screenshot method to allow root user
def screenshot(
    self,
    input,
    output_path,
    output_file="screenshot.png",
    size=(1920, 1080),
):
    """Calls Chrome or Chromium headless to take a screenshot.

    Parameters
    ----------
    - `output_file`: str
        + Name as which the screenshot will be saved.
        + File extension (e.g. .png) has to be included.
        + Default is screenshot.png
    - `input`: str
        + File or url that will be screenshotted.
        + Cannot be None
    - `size`: (int, int), optional
        + Two values representing the window size of the headless
        + browser and by extention, the screenshot size.
        + These two values must be greater than 0.
    Raises
    ------
    - `ValueError`
        + If the value of `size` is incorrect.
        + If `input` is empty.
    """

    if not input:
        raise ValueError("The `input` parameter is empty.")

    if size[0] < 1 or size[1] < 1:
        raise ValueError(
            f'Could not screenshot "{output_file}" '
            f"with a size of {size}:\n"
            "A valid size consists of two integers greater than 0."
        )

    # command used to launch chrome in
    # headless mode and take a screenshot
    command = [
        f"{self.executable}",
        "--headless",
        f"--screenshot={os.path.join(output_path, output_file)}",
        f"--window-size={size[0]},{size[1]}",
        "--no-sandbox",
        *self.flags,
        f"{input}",
    ]

    print("Executing command: " + " ".join(command))
    print("Executable: " + self.executable)

    if self.print_command:
        print(" ".join(command))

    subprocess.run(command)


html2image.browsers.chrome.ChromeHeadless.screenshot = screenshot


BASE_URL = "https://api.sportsdata.io/v3/nba"
TEMP_FOLDER = "temp"


def get_config(ti):
    """
    Loads config.txt file with API key, season year and team name.

    Returns:
        - api_key: str with API key.
        - season_year: str with season year.
        - input_team: str with team name.
    """

    config = json.load(open("./config.json"))

    ti.xcom_push(key="api_key", value=config["api_key"])
    ti.xcom_push(key="season_year", value=config["season"])
    ti.xcom_push(key="input_team", value=config["team"])


def get_main_team_info(ti):
    """
    Gets main info for a given team name, such as team id, logo, etc from SportsDataIO API.

    Flexible input: Case insensitive. Full name, name, city, or key are accepted. Ej. "Minnesota Timberwolves",
    "Timberwolves", "Minnesota", "MIN".

    Returns:
        - Key (str): with team id.
        - FullName (str): str with team full name.
        - PrimaryColor (tuple[int,int,int]): str with team primary color.
        - SecondaryColor (tuple[int,int,int]): str with team secondary color.
        - ContrastColor (tuple[int,int,int]): str with team contrast color.
        - WikipediaLogoUrl (str): str with team logo url.
    """

    # ARGS
    api_key = ti.xcom_pull(task_ids="get_config", key="api_key")
    input_team_name = ti.xcom_pull(task_ids="get_config", key="input_team")

    # FUNCTION
    url = f"{BASE_URL}/scores/json/AllTeams"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    response = requests.get(url, headers=headers)

    df = pd.DataFrame(response.json())
    df["FullName"] = df["City"] + " " + df["Name"]

    input_team_name = utils.title_case(input_team_name)

    team_info = {}
    if input_team_name in df["FullName"].values:
        team_info = df[df["FullName"] == input_team_name].to_dict("records")[0]
    elif input_team_name in df["Name"].values:
        team_info = df[df["Name"] == input_team_name].to_dict("records")[0]
    elif input_team_name in df["City"].values:
        team_info = df[df["City"] == input_team_name].to_dict("records")[0]
    elif input_team_name.upper() in df["Key"].values:
        team_info = df[df["Key"] == input_team_name.upper()].to_dict("records")[0]
    else:
        raise Exception("Input team name is not valid: " + input_team_name)

    team_info["WikipediaLogoUrl"] = utils.correct_logo_url(
        team_info["Key"], team_info["WikipediaLogoUrl"]
    )

    contrast_color = utils.contrast(
        [team_info["PrimaryColor"], team_info["SecondaryColor"]]
    )
    primary_color = utils.hex_to_rgb(team_info["PrimaryColor"])
    secondary_color = utils.hex_to_rgb(team_info["SecondaryColor"])
    mean_color = utils.mean_color([primary_color, secondary_color])

    ti.xcom_push(key="Key", value=team_info["Key"])
    ti.xcom_push(key="FullName", value=team_info["FullName"])
    ti.xcom_push(key="City", value=team_info["City"])
    ti.xcom_push(key="Name", value=team_info["Name"])
    ti.xcom_push(key="MeanColor", value=mean_color)
    ti.xcom_push(key="PrimaryColor", value=primary_color)
    ti.xcom_push(key="ContrastColor", value=contrast_color)
    ti.xcom_push(key="SecondaryColor", value=secondary_color)
    ti.xcom_push(key="WikipediaLogoUrl", value=team_info["WikipediaLogoUrl"])


# Front Page
def scrap_more_team_info(ti):
    """
    Scraps more team info from hispanosnba.com website and gets a link to the arena page.

    Returns:
    - MoreInfo  (dict): dict with more team info
    - ArenaUrl (str): link to arena page
    """

    # ARGS
    full_name = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    # FUNCTION
    base = "https://en.hispanosnba.com"
    url = f"{base}/teams/{full_name.lower().replace(' ', '-')}"
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    section = soup.find("div", {"class": "cuadro block"})

    info = {}
    for p in section.find_all("p"):
        if "Population" not in p.text:
            key, value = p.text.split(":")
            info[key.strip()] = value.strip()

            if "Arena" in key:
                info["arena_url"] = base + p.find("a")["href"]

    assert "arena_url" in info, "Arena url not found."

    ti.xcom_push(key="more_info", value=info)
    ti.xcom_push(key="arena_url", value=info["arena_url"])


def scrap_arena_images(ti):
    """
    Downloads arena images from hispanosnba.com website.

    Returns:
        - arena_urls (list): list with arena images urls.
    """
    # ARGS
    arena_url = ti.xcom_pull(task_ids="scrap_more_team_info", key="arena_url")

    # FUNCTION
    response = requests.get(arena_url)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    images = soup.find_all("img", {"class": "fotoest"})
    urls = [img["src"] for img in images][:2]

    ti.xcom_push(key="arena_urls", value=urls)


# Players Pages
def get_players_info(ti):
    """
    Gets players info for a given team id from SportsDataIO API.

    Returns: dict with players info.
    """
    # ARGS
    api_key = ti.xcom_pull(task_ids="get_config", key="api_key")
    team_key = ti.xcom_pull(task_ids="get_main_team_info", key="Key")

    # FUNCTION
    url = f"{BASE_URL}/scores/json/Players/{team_key}"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    response = requests.get(url, headers=headers)

    return response.json()


def get_players_stats(ti):
    """
    Gets players stats for a given team id and season year from SportsDataIO API.

    Returns: dict with players stats.
    """
    # ARGS
    api_key = ti.xcom_pull(task_ids="get_config", key="api_key")
    season_year = ti.xcom_pull(task_ids="get_config", key="season_year")
    team_key = ti.xcom_pull(task_ids="get_main_team_info", key="Key")

    # FUNCTION
    url = f"{BASE_URL}/stats/json/PlayerSeasonStatsByTeam/{season_year}/{team_key}"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    response = requests.get(url, headers=headers)

    return response.json()


def get_bytesio_players_images(ti):
    """
    Gets BytesIO objects for players images using multiprocessing and adds them to
    players info and stats dicts.

    Returns: dicts with added PhotoBytesIO objects.
    """
    # ARGS
    players_info_json = ti.xcom_pull(task_ids="get_players_info")
    players_stats_json = ti.xcom_pull(task_ids="get_players_stats")

    # FUNCTION
    def url_to_bytesio(url):
        """Converts url image to BytesIO object."""
        r = requests.get(url)
        return BytesIO(r.content)

    map_names_to_photo = {}

    pool = ThreadPool(len(players_info_json) // 2 + 1)
    for player in players_info_json:
        url = player["PhotoUrl"]
        player["PhotoBytesIO"] = pool.apply_async(url_to_bytesio, (url,))
        del player["PhotoUrl"]
    pool.close()
    pool.join()

    for player in players_info_json:
        player["PhotoBytesIO"] = player["PhotoBytesIO"].get()
        map_names_to_photo[player["PlayerID"]] = player["PhotoBytesIO"]

    delete = []

    for i, player in enumerate(players_stats_json):
        if player["PlayerID"] not in map_names_to_photo:
            delete.append(i)
        else:
            player["PhotoBytesIO"] = map_names_to_photo[player["PlayerID"]]

    for i in sorted(delete, reverse=True):
        del players_stats_json[i]

    ti.xcom_push(key="players_info_json", value=players_info_json)
    ti.xcom_push(key="players_stats_json", value=players_stats_json)


# Team page
def get_team_stats(ti):
    """
    Gets team stats for a given team id and season year from SportsDataIO API.

    Returns: dict with team stats.
    """
    # ARGS
    api_key = ti.xcom_pull(task_ids="get_config", key="api_key")
    season_year = ti.xcom_pull(task_ids="get_config", key="season_year")
    team_key = ti.xcom_pull(task_ids="get_main_team_info", key="Key")

    # FUNCTION
    url = f"{BASE_URL}/stats/json/TeamSeasonStats/{season_year}"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    response = requests.get(url, headers=headers)

    data = response.json()

    team_stats = next(team for team in data if team["Team"] == team_key)

    keys = [
        ("Wins", "Wins"),
        ("Losses", "Losses"),
        ("Possessions", "Possessions"),
        ("FantasyPoints", "Fantasy Points"),
        ("FreeThrowsMade", "Free Throws Made"),
        ("FieldGoalsMade", "Field Goals Made"),
        ("Points", "Points"),
        ("OffensiveRebounds", "Offensive Rebounds"),
        ("DefensiveRebounds", "Defensive Rebounds"),
        ("Assists", "Assists"),
        ("Steals", "Steals"),
        ("BlockedShots", "Blocked Shots"),
        ("Turnovers", "Turnovers"),
        ("PersonalFouls", "Personal Fouls"),
        ("TrueShootingPercentage", "True Shooting Percentage"),
    ]

    return {key[1]: team_stats[key[0]] for key in keys}


def scrap_team_record(ti):
    """
    Scraps team record from Land of Basketball website.

    Returns: pandas DataFrame with team record.
    """
    # ARGS
    full_name = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    full_name = (
        "Portland TrailBlazers" if full_name == "Portland Trail Blazers" else full_name
    )

    # FUNCTION
    url = f"https://www.landofbasketball.com/teams/records_{full_name.lower().replace(' ', '_')}.htm"

    df = pd.read_html(url)[1].iloc[2:, [0, 3, 7]]
    df[0] = df[0].str.split("-").str[0].astype(int) + 1
    return (
        df.replace("-", 0)
        .rename(columns={0: "Season", 3: "RegularW-L%", 7: "PlayoffsW-L%"})
        .reset_index(drop=True)
        .astype(
            {
                "RegularW-L%": float,
                "PlayoffsW-L%": float,
            }
        )
    )


# Prediction page
def scrap_bettings(ti):
    """
    Scraps bettings from Dratings website.

    Returns:
        - title (str): title of the game
        - date (str): date of the game
        - html_str (str): html string with bettings
        - css_str (str): css string with bettings
    """
    # ARGS
    full_name = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    # FUNCTION
    upcoming = 0
    game_found = False
    while not game_found:
        url = f"https://www.dratings.com/predictor/nba-basketball-predictions/upcoming/{upcoming}"
        response = requests.get(url)
        soup = bs4.BeautifulSoup(response.text, "html.parser")

        tables = [t for t in soup.find_all("table") if "Bet Value" in t.text]
        if tables:
            table = tables[0].tbody
            rows = table.find_all("tr")
            game_row = [row for row in rows if full_name in row.find_all("td")[1].text]
            if game_row:
                game_found = True
        upcoming += 1

    prediction_url = "https://dratings.com" + game_row[0].td.a["href"]
    response = requests.get(prediction_url)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    break_down = soup.find(id="scroll-breakdown")
    last5 = soup.find("div", {"id": "scroll-form"}).parent

    title = soup.find("h1").text
    date = soup.find("time", {"class": "time-long-heading"}).text

    css_url = (
        "https://dratings.com"
        + [l for l in soup.find_all("link", rel="stylesheet") if "main" in l["href"]][
            0
        ]["href"]
    )

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)

    css_string = requests.get(css_url).text

    # Breakdown
    html_break_down = str(break_down)
    full_name_ = full_name.replace(" ", "_")
    break_down_path = os.path.join(TEMP_FOLDER, f"breakdown_{full_name_}.png")
    hmi = html2image.Html2Image(output_path=TEMP_FOLDER)
    hmi.screenshot(
        html_str=html_break_down,
        css_str=css_string,
        save_as=f"breakdown_{full_name_}.png",
        size=(1100, 280),
    )

    # Last 5 games
    html_last5 = (
        """<body><div class="layout"><div class="cell">"""
        + str(last5)
        + """</div></div></body>"""
    )
    last5_path = os.path.join(TEMP_FOLDER, f"last5_{full_name_}.png")
    hmi.screenshot(
        html_str=html_last5,
        css_str=css_string + """body{background-color: white;padding-top: 10px;}""",
        save_as=f"last5_{full_name_}.png",
        size=(1100, 570),
    )

    ti.xcom_push(key="title", value=title)
    ti.xcom_push(key="date", value=date)
    ti.xcom_push(key="break_down_path", value=break_down_path)
    ti.xcom_push(key="last5_path", value=last5_path)
