from googleapiclient.discovery import build
import pandas as pd
from sqlalchemy import create_engine
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
REGION_CODE = 'IN'
MAX_RESULTS = 10
DB_NAME = 'youtube_trending.db'


# Creating YouTube service
youtube = build('youtube', 'v3', developerKey=API_KEY)

def get_trending_videos(region_code='IN', max_results=10):
    request = youtube.videos().list(
        part="snippet,statistics",
        chart="mostPopular",
        regionCode=region_code,
        maxResults=max_results
    )
    response = request.execute()

    videos = []
    for item in response['items']:
        video = {
            'title': item['snippet']['title'],
            'channel': item['snippet']['channelTitle'],
            'published_at': item['snippet']['publishedAt'],
            'views': item['statistics'].get('viewCount', 0),
            'likes': item['statistics'].get('likeCount', 0),
            'comments': item['statistics'].get('commentCount', 0),
            'date_collected': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        videos.append(video)

    return pd.DataFrame(videos)

def store_to_db(df, db_name='youtube_trending.db'):
    engine = create_engine(f'sqlite:///{db_name}')
    df.to_sql('trending_videos', con=engine, if_exists='append', index=False)
    print(f"Stored {len(df)} rows to {db_name}")

def main():
    df = get_trending_videos(REGION_CODE, MAX_RESULTS)
    print(df.head())
    store_to_db(df, DB_NAME)

if __name__ == '__main__':
    main()
