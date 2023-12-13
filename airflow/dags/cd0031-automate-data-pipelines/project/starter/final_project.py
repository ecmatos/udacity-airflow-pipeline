from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': pendulum.duration(seconds=300)
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    end_operator = DummyOperator(task_id='End_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table = "staging_events",
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        s3_bucket = "udacity-dend",
        s3_key = "log-data/2018/11",
        copy_json_option = "s3://udacity-dend/log_json_path.json",
        region = 'us-west-2'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table = "staging_songs",
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        s3_bucket = "esron-lab",
        s3_key = "song-data",
        copy_json_option = 'auto',
        region = 'us-east-1'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table = "songplays",
        redshift_conn_id = "redshift",
        query = SqlQueries.songplay_table_insert,
        append_only = False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table = "dim_user",
        redshift_conn_id = "redshift",
        query = SqlQueries.user_table_insert,
        append_only = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table = "dim_song",
        redshift_conn_id = "redshift",
        query = SqlQueries.song_table_insert,
        append_only = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table = "dim_artist",
        redshift_conn_id = "redshift",
        query = SqlQueries.artist_table_insert,
        append_only = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table = "dim_time",
        redshift_conn_id = "redshift",
        query = SqlQueries.time_table_insert,
        append_only = False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        tables = ["staging_events", "staging_songs", "songplays", "dim_user",
            "dim_song", "dim_artist", "dim_time"]
    )


    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()