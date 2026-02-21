import psycopg2
import os
from dotenv import load_dotenv
from video_stat import extract_video_data, get_video_ids, get_playlist_id

playlist_id = get_playlist_id()
video_ids = get_video_ids(playlist_id)
video_data = extract_video_data(video_ids)

# then insert video_data into Postgres


load_dotenv()   

#load_dotenv(dotenv_path= './.env')
#print(os.getenv("DB_PASSWORD"))
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),  # can be empty
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)
cur = conn.cursor()


insert_query = """
INSERT INTO youtube_videos (
    video_id,
    title,
    published_at,
    duration,
    view_count,
    like_count,
    comment_count
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (video_id) DO NOTHING;
"""

for video in video_data:
    cur.execute(insert_query, (
        video["video_id"],
        video["title"],
        video["publishedAt"],
        video["duration"],
        video["viewCount"],
        video["likeCount"],
        video["commentCount"],
    ))

conn.commit()
cur.close()
conn.close()