from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from API.video_stat import get_playlist_id, get_video_ids, extract_video_data, save_data

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



with DAG(
     dag_id="produce_json",
     default_args = default_args,
     description = 'DAG to prodice JSON file with raw data',
     schedule="0 14 * * *",
     catchup = False
 ) as dag:
    
    #define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json = save_data(extract_data)

    #define dependencies

    playlist_id >> video_ids>>extract_data>>save_to_json


