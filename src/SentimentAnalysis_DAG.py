from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

from src.Credentials import YOUTUBE_API_KEY
from src.Credentials import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT

from ETL_Functions.Extract_Functions import (
    Extract_YT_Comments,
    Extract_Reddit_Comments
)
import ETL_Functions.Transform_Functions
import ETL_Functions.Load_Functions

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 20),  # Set a suitable start date
    'retries': 1,
}

# Define the DAG
with DAG(
    'Sentiment Analysis_ETL',
    default_args = default_args,
    description = 'ETL pipeline for sentiment analysis',
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    # Task: Extract YouTube comments
    def Extract_Comments_from_YouTube():
        from googleapiclient.discovery import build

        youtube = build('youtube', 'v3', developerKey = YOUTUBE_API_KEY)
        video_id = 'FJF0GOaOWk4'

        return Extract_YT_Comments(video_id, youtube)
    
    # Task: Extract Reddit comments
    def Extract_Comments_from_Reddit():
        import praw

        reddit = praw.Reddit(
            client_id = REDDIT_CLIENT_ID,
            client_secret = REDDIT_CLIENT_SECRET,
            user_agent = REDDIT_USER_AGENT
        )
        post_url = 'https://www.reddit.com/r/movies/comments/1ff3566/transformers_one_review_thread/'

        return Extract_Reddit_Comments(post_url, reddit)