from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import extract
import transform
import load


# CHANGE TO RUN ALL DAGS OR JUST ONE WITH TEAM SET IN CONFIG
run_all = True


default_args = {
    "owner": "Sergio Herreros",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def bind_function(name: str, team: str):
    """
    Creates a personalized config function for each team.
    """

    def get_config(ti):
        import json

        with open("config.json", "r") as f:
            config = json.load(f)

        config["team"] = team

        ti.xcom_push(key="api_key", value=config["api_key"])
        ti.xcom_push(key="season_year", value=config["season"])
        ti.xcom_push(key="input_team", value=team)

    get_config.__name__ = name + "_" + team.lower().replace(" ", "_")
    return get_config


def create_all_dags():
    """Creates one DAG per team which will be run once on start."""

    teams = [
        "Atlanta Hawks",
        "Boston Celtics",
        "Brooklyn Nets",
        "Charlotte Hornets",
        "Chicago Bulls",
        "Cleveland Cavaliers",
        "Dallas Mavericks",
        "Denver Nuggets",
        "Detroit Pistons",
        "Golden State Warriors",
        "Houston Rockets",
        "Indiana Pacers",
        "Los Angeles Clippers",
        "Los Angeles Lakers",
        "Memphis Grizzlies",
        "Miami Heat",
        "Milwaukee Bucks",
        "Minnesota Timberwolves",
        "New Orleans Pelicans",
        "New York Knicks",
        "Oklahoma City Thunder",
        "Orlando Magic",
        "Philadelphia 76ers",
        "Phoenix Suns",
        "Portland Trail Blazers",
        "Sacramento Kings",
        "San Antonio Spurs",
        "Toronto Raptors",
        "Utah Jazz",
        "Washington Wizards",
    ]

    dags = []

    for team in teams:
        # run once on start
        with DAG(
            start_date=datetime(2021, 1, 1),
            dag_id=f"nba_dag_{team.lower().replace(' ', '_')}",
            default_args=default_args,
            schedule_interval="@once",
            catchup=False,
        ) as dag:
            get_config_task = PythonOperator(
                task_id="get_config",
                python_callable=bind_function("get_config", team),
            )

            get_main_team_info_task = PythonOperator(
                task_id="get_main_team_info",
                python_callable=extract.get_main_team_info,
            )

            scrap_more_team_info_task = PythonOperator(
                task_id="scrap_more_team_info",
                python_callable=extract.scrap_more_team_info,
            )

            scrap_arena_images_task = PythonOperator(
                task_id="scrap_arena_images",
                python_callable=extract.scrap_arena_images,
            )

            get_players_info_task = PythonOperator(
                task_id="get_players_info",
                python_callable=extract.get_players_info,
            )

            get_players_stats_task = PythonOperator(
                task_id="get_players_stats",
                python_callable=extract.get_players_stats,
            )

            get_bytesio_players_images_task = PythonOperator(
                task_id="get_bytesio_players_images",
                python_callable=extract.get_bytesio_players_images,
            )

            get_team_stats_task = PythonOperator(
                task_id="get_team_stats",
                python_callable=extract.get_team_stats,
            )

            scrap_team_record_task = PythonOperator(
                task_id="scrap_team_record",
                python_callable=extract.scrap_team_record,
            )

            scrap_bettings_task = PythonOperator(
                task_id="scrap_bettings",
                python_callable=extract.scrap_bettings,
            )

            transform_players_info_task = PythonOperator(
                task_id="transform_players_info",
                python_callable=transform.transform_players_info,
            )

            transform_players_stats_task = PythonOperator(
                task_id="transform_players_stats",
                python_callable=transform.transform_players_stats,
            )

            load_front_page_task = PythonOperator(
                task_id="load_front_page",
                python_callable=load.front_page,
            )

            load_individual_info_task = PythonOperator(
                task_id="load_individual_info",
                python_callable=load.individual_info_page,
            )

            load_individual_stats_task = PythonOperator(
                task_id="load_individual_stats",
                python_callable=load.individual_stats_page,
            )

            load_team_stats_task = PythonOperator(
                task_id="load_team_stats",
                python_callable=load.team_stats_page,
            )

            load_prediction_task = PythonOperator(
                task_id="load_prediction",
                python_callable=load.prediction_page,
            )

            merge_pdfs_task = PythonOperator(
                task_id="merge_pdfs",
                python_callable=load.merge_pdfs,
            )

            get_config_task >> get_main_team_info_task
            get_main_team_info_task >> [
                scrap_more_team_info_task,
                get_players_info_task,
                get_players_stats_task,
                get_team_stats_task,
                scrap_team_record_task,
                scrap_bettings_task,
            ]

            scrap_more_team_info_task >> scrap_arena_images_task >> load_front_page_task

            get_players_info_task >> get_bytesio_players_images_task
            get_players_stats_task >> get_bytesio_players_images_task
            get_bytesio_players_images_task >> transform_players_info_task
            get_bytesio_players_images_task >> transform_players_stats_task
            transform_players_info_task >> load_individual_info_task
            transform_players_stats_task >> load_individual_stats_task

            get_team_stats_task >> load_team_stats_task
            scrap_team_record_task >> load_team_stats_task
            scrap_bettings_task >> load_prediction_task

            load_front_page_task >> merge_pdfs_task
            load_individual_info_task >> merge_pdfs_task
            load_individual_stats_task >> merge_pdfs_task
            load_team_stats_task >> merge_pdfs_task
            load_prediction_task >> merge_pdfs_task

            dags.append(dag)

    for dag in dags:
        globals()[dag.dag_id] = dag


def create_dag():
    """Create a DAG to run the NBA ETL pipeline that will run every day at 3pm."""
    with DAG(
        dag_id="nba-dag",
        schedule_interval="0 15 * * *",
        start_date=datetime(2021, 9, 20),
        catchup=False,
        default_args=default_args,
    ) as dag:
        get_config_task = PythonOperator(
            task_id="get_config",
            python_callable=extract.get_config,
        )

        get_main_team_info_task = PythonOperator(
            task_id="get_main_team_info",
            python_callable=extract.get_main_team_info,
        )

        scrap_more_team_info_task = PythonOperator(
            task_id="scrap_more_team_info",
            python_callable=extract.scrap_more_team_info,
        )

        scrap_arena_images_task = PythonOperator(
            task_id="scrap_arena_images",
            python_callable=extract.scrap_arena_images,
        )

        get_players_info_task = PythonOperator(
            task_id="get_players_info",
            python_callable=extract.get_players_info,
        )

        get_players_stats_task = PythonOperator(
            task_id="get_players_stats",
            python_callable=extract.get_players_stats,
        )

        get_bytesio_players_images_task = PythonOperator(
            task_id="get_bytesio_players_images",
            python_callable=extract.get_bytesio_players_images,
        )

        get_team_stats_task = PythonOperator(
            task_id="get_team_stats",
            python_callable=extract.get_team_stats,
        )

        scrap_team_record_task = PythonOperator(
            task_id="scrap_team_record",
            python_callable=extract.scrap_team_record,
        )

        scrap_bettings_task = PythonOperator(
            task_id="scrap_bettings",
            python_callable=extract.scrap_bettings,
        )

        transform_players_info_task = PythonOperator(
            task_id="transform_players_info",
            python_callable=transform.transform_players_info,
        )

        transform_players_stats_task = PythonOperator(
            task_id="transform_players_stats",
            python_callable=transform.transform_players_stats,
        )

        load_front_page_task = PythonOperator(
            task_id="load_front_page",
            python_callable=load.front_page,
        )

        load_individual_info_task = PythonOperator(
            task_id="load_individual_info",
            python_callable=load.individual_info_page,
        )

        load_individual_stats_task = PythonOperator(
            task_id="load_individual_stats",
            python_callable=load.individual_stats_page,
        )

        load_team_stats_task = PythonOperator(
            task_id="load_team_stats",
            python_callable=load.team_stats_page,
        )

        load_prediction_task = PythonOperator(
            task_id="load_prediction",
            python_callable=load.prediction_page,
        )

        merge_pdfs_task = PythonOperator(
            task_id="merge_pdfs",
            python_callable=load.merge_pdfs,
        )

        get_config_task >> get_main_team_info_task
        get_main_team_info_task >> [
            scrap_more_team_info_task,
            get_players_info_task,
            get_players_stats_task,
            get_team_stats_task,
            scrap_team_record_task,
            scrap_bettings_task,
        ]

        scrap_more_team_info_task >> scrap_arena_images_task >> load_front_page_task

        get_players_info_task >> get_bytesio_players_images_task
        get_players_stats_task >> get_bytesio_players_images_task
        get_bytesio_players_images_task >> transform_players_info_task
        get_bytesio_players_images_task >> transform_players_stats_task
        transform_players_info_task >> load_individual_info_task
        transform_players_stats_task >> load_individual_stats_task

        get_team_stats_task >> load_team_stats_task
        scrap_team_record_task >> load_team_stats_task
        scrap_bettings_task >> load_prediction_task

        load_front_page_task >> merge_pdfs_task
        load_individual_info_task >> merge_pdfs_task
        load_individual_stats_task >> merge_pdfs_task
        load_team_stats_task >> merge_pdfs_task
        load_prediction_task >> merge_pdfs_task

    globals()[dag.dag_id] = dag


if run_all:
    create_all_dags()
else:
    create_dag()
