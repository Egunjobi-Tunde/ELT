from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from API.video_stat import get_playlist_id, get_video_ids, extract_video_data, save_data
from datawarehouse.dwh import core_table, staging_table
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
local_tz = pendulum.timezone('Africa/Lagos')

# Default Args
default_args = {
    "owner": "Fahd",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "egunjobitunde20@gmail.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"

with DAG(
     dag_id="produce_json",
     default_args = default_args,
     description = 'DAG to prodice JSON file with raw data',
     schedule="0 14 * * *",
     catchup = False
 ) as dag_produce:
    
    #define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json = save_data(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )
    #define dependencies

    playlist_id >> video_ids>>extract_data>>save_to_json>> trigger_update_db


with DAG(
     dag_id="update_db",
     default_args = default_args,
     description = 'DAG to process Json file and insert and update Data to Core and staging db',
     schedule=None,
     catchup = False
 ) as dag_update:
    
    #define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Define dependencies
    update_staging >> update_core >> trigger_data_quality

# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core
