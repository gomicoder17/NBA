from io import BytesIO
import os
from fpdf import FPDF
from gradient_generator import generate_gradient
from PyPDF2 import PdfMerger
from matplotlib.pyplot import subplots
from pandas import DataFrame

TEMP_FOLDER = "temp"


def front_page(ti) -> str:
    """
    Generates front page of the report.

    Returns: pdf path (str).
    """
    # ARGS
    contrast_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="ContrastColor"
    )
    mean_color: str = ti.xcom_pull(task_ids="get_main_team_info", key="MeanColor")
    primary_color: str = ti.xcom_pull(task_ids="get_main_team_info", key="PrimaryColor")
    secondary_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="SecondaryColor"
    )
    wikipedia_logo_url: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="WikipediaLogoUrl"
    )
    city: str = ti.xcom_pull(task_ids="get_main_team_info", key="City")
    name: str = ti.xcom_pull(task_ids="get_main_team_info", key="Name")
    full_name: str = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")
    more_info: dict[str, str] = ti.xcom_pull(
        task_ids="scrap_more_team_info", key="more_info"
    )
    arena_urls: list[str] = ti.xcom_pull(
        task_ids="scrap_arena_images", key="arena_urls"
    )

    # FUNCTION
    full_name_ = full_name.replace(" ", "_")

    pdf = FPDF()

    pdf.add_font("FreeSans", "", "./fonts/FreeSans.ttf")
    pdf.add_font("FreeSans", "B", "./fonts/FreeSansBold.ttf")

    font_family = "FreeSans"
    title_font_size = 56
    text_font_size = 16
    text_color = contrast_color

    pdf.add_page()

    # Background
    primary = primary_color
    secondary = secondary_color
    background = generate_gradient(
        round(pdf.w), round(pdf.h), [primary, secondary], seed=0
    )
    pdf.image(background, 0, 0, pdf.w, pdf.h)

    # Title
    pad_y = 20
    pdf.set_font(font_family, "B", title_font_size)
    pdf.set_text_color(*text_color)
    pdf.ln(10)
    pdf.cell(w=pdf.epw, h=pad_y, txt=city, align="C")
    pdf.ln()
    pdf.cell(w=pdf.epw, h=pad_y, txt=name, align="C")
    pdf.ln()

    # Logo
    hori = ["Lakers", "Clippers", "Spurs"]
    logo_h = 55 if name in hori else 65
    logo_w = logo_h * 3 // 2 if name in hori else logo_h
    pdf.ln(5)
    pdf.image(
        wikipedia_logo_url,
        x=(pdf.w - logo_w) / 2,
        y=pdf.y,
        w=logo_w,
        h=logo_h,
    )
    pdf.ln(logo_h)

    # Arena image exterior
    arena_w, arena_h = 70, 52
    pdf.ln(15)
    pdf.image(
        arena_urls[0],
        x=(pdf.w / 2 - arena_w) / 2,
        y=pdf.y,
        w=arena_w,
        h=arena_h,
    )
    arena_1_y = pdf.y

    # More info 1
    margin_x = -5
    margin_box = 2
    cell_height = arena_h / 4
    info1 = ["City", "Arena", "Owner/s", "General Manager"]
    # Background rectangle with rounded corners
    pdf.set_fill_color(*mean_color)
    pdf.rect(
        pdf.w / 2 + margin_x - 2 * margin_box,
        pdf.y - margin_box,
        w=pdf.epw - (pdf.w / 2 + margin_x) + 5 * margin_box + 2,
        h=cell_height * len(info1) + margin_box * 2,
        round_corners=True,
        style="DF",
    )
    pdf.set_text_color(*text_color)
    pdf.set_font(font_family, "B", text_font_size)
    for info in info1:
        pdf.set_xy(pdf.w / 2 + margin_x, pdf.y)
        pdf.cell(
            h=cell_height,
            txt=info + ":",
        )
        pdf.set_font(font_family, "", text_font_size)
        pdf.multi_cell(
            h=cell_height,
            txt=more_info[info],
            w=pdf.epw - pdf.x + pdf.r_margin / 2,
            max_line_height=cell_height / 2,
        )
        pdf.set_font(font_family, "B", text_font_size)

    # Arena image interior
    pdf.set_y(arena_1_y + arena_h)
    pdf.ln(20)
    margin_x = 5
    pdf.image(
        arena_urls[1],
        x=pdf.w / 2 + margin_x,
        y=pdf.y,
        w=arena_w,
        h=arena_h,
    )

    # More info 2
    margin_x = 10
    info2 = [
        "NBA titles",
        "Conference titles",
        "Division titles",
        "Playoff appearences",
    ]
    # Background rectangle with rounded corners
    pdf.set_fill_color(*mean_color)
    pdf.rect(
        (pdf.w / 2 - arena_w) / 2 + margin_x - 2 * margin_box,
        pdf.y - margin_box,
        w=pdf.epw // 2 - ((pdf.w / 2 - arena_w) / 2 + margin_x) + 5 * margin_box,
        h=cell_height * len(info2) + margin_box * 2,
        round_corners=True,
        style="DF",
    )
    pdf.set_text_color(*text_color)
    pdf.set_font(font_family, "B", text_font_size)
    for info in info2:
        pdf.set_xy((pdf.w / 2 - arena_w) / 2 + margin_x, pdf.y)
        pdf.cell(
            h=cell_height,
            txt=info + ":",
        )
        pdf.set_font(font_family, "", text_font_size)
        pdf.cell(
            h=cell_height,
            txt=more_info[info],
        )
        pdf.ln()
        pdf.set_font(font_family, "B", text_font_size)

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
    pdf_path = f"{TEMP_FOLDER}/front_page_{full_name_}.pdf"

    pdf.output(pdf_path)

    return pdf_path


def individual_info_page(ti) -> str:
    """
    Generates individual stats page of the report.

    Returns: path to the generated pdf file (str).
    """
    # ARGS
    contrast_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="ContrastColor"
    )
    primary_color: str = ti.xcom_pull(task_ids="get_main_team_info", key="PrimaryColor")
    secondary_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="SecondaryColor"
    )
    player_info_df: DataFrame = ti.xcom_pull(task_ids="transform_players_info")
    full_name: str = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    # FUNCTION
    full_name_ = full_name.replace(" ", "_")

    pdf = FPDF()

    pdf.add_font("FreeSans", "", "./fonts/FreeSans.ttf")
    pdf.add_font("FreeSans", "B", "./fonts/FreeSansBold.ttf")

    font_family = "FreeSans"
    title_font_size = 36
    text_font_size = 10
    text_color = contrast_color
    pdf.add_page()

    # Background
    primary = primary_color
    secondary = secondary_color
    background = generate_gradient(round(pdf.w), round(pdf.h), [primary, secondary])

    pdf.image(background, 0, 0, pdf.w, pdf.h)

    # Title
    pad_y = 8
    pdf.set_font(font_family, "B", title_font_size)
    pdf.set_text_color(*text_color)
    pdf.cell(w=pdf.epw, h=pad_y, txt="Players Info", align="C")
    pdf.ln()

    # Table
    pdf.ln(10)
    row_height = (pdf.eph - pad_y - 10) // (player_info_df.shape[0] + 1)
    image_width = row_height * 1.2

    # Auto column width
    col_widths = []

    for col in player_info_df.columns:
        if col != "PhotoBytesIO":
            col_widths.append(
                max(max(len(str(x)), len(col) + 4) for x in player_info_df[col])
            )
    col_widths = [(pdf.epw - image_width) * w / sum(col_widths) for w in col_widths]
    col_widths = [image_width] + col_widths

    # HEADER
    pdf.set_font(font_family, "B", text_font_size)
    th = pdf.font_size
    for i, col in enumerate(player_info_df.columns):
        text = col if col != "PhotoBytesIO" else ""
        pdf.cell(w=col_widths[i], h=row_height, txt=text, align="C", border=1)
    pdf.ln(row_height)

    # BODY
    pdf.set_font(font_family, "", text_font_size)
    th = pdf.font_size
    for _, row in player_info_df.iterrows():
        x, y = pdf.x, pdf.y
        pad = 3
        pdf.image(
            name=row["PhotoBytesIO"],
            w=col_widths[0] - 2 * pad,
            h=row_height - pad // 2,
            x=pdf.x + pad,
            y=pdf.y + pad // 4,
        )

        pdf.rect(pdf.x, pdf.y, col_widths[0], row_height, "D")
        pdf.x += col_widths[0]

        for i, val in enumerate(row):
            if i > 0:
                text = str(val)
                pdf.cell(w=col_widths[i], h=row_height, txt=text, align="C", border=1)
        pdf.ln(row_height)

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
    pdf_path = f"{TEMP_FOLDER}/individual_info_{full_name_}.pdf"

    pdf.output(pdf_path)
    return pdf_path


def individual_stats_page(ti) -> str:
    """
    Generates individual stats page of the report.

    Returns: path to the generated pdf file (str).
    """
    # ARGS
    contrast_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="ContrastColor"
    )
    primary_color: str = ti.xcom_pull(task_ids="get_main_team_info", key="PrimaryColor")
    secondary_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="SecondaryColor"
    )
    player_stats_df: DataFrame = ti.xcom_pull(task_ids="transform_players_stats")
    full_name: str = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    # FUNCTION
    full_name_ = full_name.replace(" ", "_")

    pdf = FPDF()

    pdf.add_font("FreeSans", "", "./fonts/FreeSans.ttf")
    pdf.add_font("FreeSans", "B", "./fonts/FreeSansBold.ttf")

    font_family = "FreeSans"
    title_font_size = 36
    text_font_size = 10
    text_color = contrast_color

    pdf.add_page()

    # Background
    primary = primary_color
    secondary = secondary_color
    background = generate_gradient(round(pdf.w), round(pdf.h), [primary, secondary])

    pdf.image(background, 0, 0, pdf.w, pdf.h)

    # Title
    pad_y = 8
    pdf.set_font(font_family, "B", title_font_size)
    pdf.set_text_color(*text_color)
    pdf.cell(w=pdf.epw, h=pad_y, txt="Players Stats", align="C")
    pdf.ln()

    # Table
    pdf.ln(10)
    row_height = (pdf.eph - pad_y - 10) // (player_stats_df.shape[0] + 1)
    image_width = row_height * 1.2

    # Auto column width
    col_widths = []

    for col in player_stats_df.columns:
        if col != "PhotoBytesIO":
            col_widths.append(
                max(max(len(str(x)), len(col) + 4) for x in player_stats_df[col])
            )
    col_widths = [(pdf.epw - image_width) * w / sum(col_widths) for w in col_widths]
    col_widths = [image_width] + col_widths

    # HEADER
    pdf.set_font(font_family, "B", text_font_size)
    th = pdf.font_size
    for i, col in enumerate(player_stats_df.columns):
        text = col if col != "PhotoBytesIO" else ""
        pdf.cell(w=col_widths[i], h=row_height, txt=text, align="C", border=1)
    pdf.ln(row_height)

    # BODY
    pdf.set_font(font_family, "", text_font_size)
    th = pdf.font_size
    for _, row in player_stats_df.iterrows():
        x, y = pdf.x, pdf.y
        pad = 3
        pdf.image(
            name=row["PhotoBytesIO"],
            w=col_widths[0] - 2 * pad,
            h=row_height - pad // 2,
            x=pdf.x + pad,
            y=pdf.y + pad // 4,
        )

        pdf.rect(pdf.x, pdf.y, col_widths[0], row_height, "D")
        pdf.x += col_widths[0]

        for i, val in enumerate(row):
            if i > 0:
                text = (
                    str(val)
                    if i != 1
                    else val.split(" ")[0][0] + ". " + val.split(" ")[1]
                )
                pdf.cell(w=col_widths[i], h=row_height, txt=text, align="C", border=1)
        pdf.ln(row_height)

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
    pdf_path = f"{TEMP_FOLDER}/individual_stats_{full_name_}.pdf"

    pdf.output(pdf_path)

    return pdf_path


def team_stats_page(ti) -> str:
    """
    Generates team stats page of the report.

    Returns: path to the PDF file (str)
    """
    # ARGS
    contrast_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="ContrastColor"
    )
    primary_color: str = ti.xcom_pull(task_ids="get_main_team_info", key="PrimaryColor")
    secondary_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="SecondaryColor"
    )
    team_stats: dict = ti.xcom_pull(task_ids="get_team_stats")
    team_record: DataFrame = ti.xcom_pull(task_ids="scrap_team_record")
    full_name: str = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    # FUNCTION
    full_name_ = full_name.replace(" ", "_")

    pdf = FPDF()

    pdf.add_font("FreeSans", "", "./fonts/FreeSans.ttf")
    pdf.add_font("FreeSans", "B", "./fonts/FreeSansBold.ttf")

    font_family = "FreeSans"
    title_font_size = 36
    text_font_size = 12
    text_color = contrast_color

    pdf.add_page()

    # Background
    primary = primary_color
    secondary = secondary_color
    background = generate_gradient(round(pdf.w), round(pdf.h), [primary, secondary])

    pdf.image(background, 0, 0, pdf.w, pdf.h)

    # Title
    pad_y = 8
    pdf.set_font(font_family, "B", title_font_size)
    pdf.set_text_color(*text_color)
    pdf.cell(w=pdf.epw, h=pad_y, txt="Team Stats", align="C")
    pdf.ln()

    # Table (Stat: Value)
    pdf.ln(10)
    title_table_h = pdf.eph * 0.65
    row_height = (title_table_h - pad_y - 10) // len(team_stats)
    col_widths = [pdf.epw // 2, pdf.epw // 2]

    pdf.set_font(font_family, "", text_font_size)
    for stat, val in team_stats.items():
        for i, text in enumerate([stat, val]):
            pdf.cell(w=col_widths[i], h=row_height, txt=str(text), align="C", border=1)
        pdf.ln(row_height)

    # Record TimeSeries Plot

    team_record.sort_values(by="Season", inplace=True)

    fig, ax = subplots(figsize=(15, 6))
    regular = team_record[team_record["RegularW-L%"] != 0][["Season", "RegularW-L%"]]
    ax.plot(regular["Season"], regular["RegularW-L%"], label="Regular W/L %")
    playoffs = team_record[team_record["PlayoffsW-L%"] != 0][["Season", "PlayoffsW-L%"]]
    ax.plot(playoffs["Season"], playoffs["PlayoffsW-L%"], label="Playoffs W/L %")

    ax.set_title("Team Record: Win-Loss Percentage", {"fontsize": 24})
    ax.set_xticklabels([int(e) for e in ax.get_xticks()], rotation=45, fontsize=14)
    ax.set_yticklabels([f"{i:.0%}" for i in ax.get_yticks()], fontsize=14)
    ax.legend(fontsize=16)

    fig.tight_layout()

    # Save plot to BytesIO
    pdf.ln(10)
    y_margin = 10
    buf = BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    pdf.image(
        buf,
        x=pdf.x,
        y=pdf.y + y_margin,
        w=pdf.epw,
        h=pdf.eph - title_table_h - 2 * y_margin,
    )
    buf.close()

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
    pdf_path = f"{TEMP_FOLDER}/team_stats_{full_name_}.pdf"

    pdf.output(pdf_path)

    return pdf_path


def prediction_page(ti) -> str:
    """
    Generates prediction page of the report.

    Returns: path to the PDF file (str)
    """
    # ARGS
    contrast_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="ContrastColor"
    )
    primary_color: str = ti.xcom_pull(task_ids="get_main_team_info", key="PrimaryColor")
    secondary_color: str = ti.xcom_pull(
        task_ids="get_main_team_info", key="SecondaryColor"
    )
    full_name: str = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")
    title: str = ti.xcom_pull(task_ids="scrap_bettings", key="title")
    date: str = ti.xcom_pull(task_ids="scrap_bettings", key="date")
    break_down_path: str = ti.xcom_pull(
        task_ids="scrap_bettings", key="break_down_path"
    )
    last5_path: str = ti.xcom_pull(task_ids="scrap_bettings", key="last5_path")

    # FUNCTION
    full_name_ = full_name.replace(" ", "_")

    pdf = FPDF()

    pdf.add_font("FreeSans", "", "./fonts/FreeSans.ttf")
    pdf.add_font("FreeSans", "B", "./fonts/FreeSansBold.ttf")

    font_family = "FreeSans"
    title_font_size = 36
    text_font_size = 24
    text_color = contrast_color

    pdf.add_page()

    # Background
    primary = primary_color
    secondary = secondary_color
    background = generate_gradient(round(pdf.w), round(pdf.h), [primary, secondary])

    pdf.image(background, 0, 0, pdf.w, pdf.h)

    # Title
    pad_y = 20
    pdf.set_font(font_family, "B", title_font_size)
    pdf.set_text_color(*text_color)
    pdf.cell(w=pdf.epw, h=pad_y, txt=title, align="C")
    pdf.ln()

    # Date
    pdf.ln(3)
    pad_y = 10
    pdf.set_font(font_family, "B", text_font_size)
    pdf.set_text_color(*text_color)
    pdf.cell(w=pdf.epw, h=pad_y, txt=date, align="C")
    pdf.ln()

    # Breakdown
    pdf.ln(10)
    pdf.image(
        break_down_path,
        x=pdf.x,
        y=pdf.y,
        w=pdf.epw,
    )
    pdf.ln(60)

    # Last 5
    pdf.ln(10)
    pdf.image(
        last5_path,
        x=pdf.x,
        y=pdf.y,
        w=pdf.epw,
    )

    if not os.path.exists(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
    pdf_path = f"{TEMP_FOLDER}/prediction_{full_name_}.pdf"

    pdf.output(pdf_path)

    return pdf_path


def merge_pdfs(ti):
    """
    Merges PDFs into one.

    Returns: path to the merged PDF (str)
    """
    # ARGS
    front_page_path: str = ti.xcom_pull(task_ids="load_front_page")
    individual_info_path: str = ti.xcom_pull(task_ids="load_individual_info")
    individual_stats_path: str = ti.xcom_pull(task_ids="load_individual_stats")
    team_stats_path: str = ti.xcom_pull(task_ids="load_team_stats")
    prediction_path: str = ti.xcom_pull(task_ids="load_prediction")
    full_name: str = ti.xcom_pull(task_ids="get_main_team_info", key="FullName")

    # FUNCTION
    pdfs = [
        front_page_path,
        individual_info_path,
        individual_stats_path,
        team_stats_path,
        prediction_path,
    ]

    merger = PdfMerger()

    for pdf in pdfs:
        merger.append(pdf)

    if not os.path.exists("out"):
        try:
            os.mkdir("out")
        except:
            msg = "You deleted the /out folder while the container was running. Please create it again or rebuild the container."
            raise FileNotFoundError(msg)

    full_name_ = full_name.replace(" ", "_")

    out_path = f"out/{full_name_}.pdf"

    merger.write(out_path)
    merger.close()

    return out_path
