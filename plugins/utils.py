def hex_to_rgb(hex: str) -> tuple:
    """Converts a hex color to rgb"""
    hex = hex.lstrip("#")
    hlen = len(hex)
    return tuple(int(hex[i : i + hlen // 3], 16) for i in range(0, hlen, hlen // 3))


def mean_color(colors: list) -> tuple:
    """Returns the mean color of a list of colors (r, g, b)"""
    mean = (0, 0, 0)
    for color in colors:
        r, g, b = color
        mean = (mean[0] + r, mean[1] + g, mean[2] + b)
    mean = (mean[0] / len(colors), mean[1] / len(colors), mean[2] / len(colors))
    return mean


def contrast(colors: list) -> tuple:
    """Returns the contrast color of a list of colors (in hex)"""
    colors = [hex_to_rgb(color) for color in colors]
    r, g, b = mean_color(colors)
    return (0, 0, 0) if (r * 0.299 + g * 0.587 + b * 0.114) > 186 else (255, 255, 255)


def correct_logo_url(team_key: str, wikipedia_logo_url: str) -> str:
    """Returns the correct logo url for a team"""
    if team_key == "ATL":
        return "https://i0.wp.com/sportytell.com/wp-content/uploads/2020/11/NBA-atlanta-hawks-Logo.png?resize=300%2C300&ssl=1"
    elif team_key == "TOR":
        return "https://i1.wp.com/sportytell.com/wp-content/uploads/2020/11/NBA-toronto-raptors-Logo.png?resize=300%2C300&ssl=1"
    elif team_key == "UTA":
        return "https://i1.wp.com/sportytell.com/wp-content/uploads/2020/11/NBA-utah-jazz-Logo.png?resize=300%2C300&ssl=1"
    elif team_key == "HOU":
        return "https://i0.wp.com/sportytell.com/wp-content/uploads/2020/11/NBA-houston-rockets-Logo.png?resize=300%2C300&ssl=1"
    elif team_key == "NO":
        return "https://i2.wp.com/sportytell.com/wp-content/uploads/2020/11/NBA-new-orleans-pelicans-Logo.png?resize=300%2C300&ssl=1"
    return wikipedia_logo_url


def title_case(s: str) -> str:
    """Returns a string in title case"""
    return " ".join(word[0].upper() + word[1:] for word in s.split())
