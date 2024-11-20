from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

from src.Credentials import YOUTUBE_API_KEY
from src.Credentials import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT

from ETL_Functions.Extract_Functions import (
    Get_Next_Video_ID,
    Get_Next_Reddit_Post_URL,
    Extract_YT_Comments,
    Extract_Reddit_Comments
)

from ETL_Functions.Transform_Functions import (
    YT_Convert_Emoji_to_Text,
    RDT_Convert_Emoji_to_Text,
    YT_HandleHTML,
    RDT_HandleHTML,
    YTcomments_to_Dataframe,
    RDTcomments_to_Dataframe,
    YTcolumn_to_string,
    RDTcolumn_to_string,
    remove_first_character_in_username,
    Sentiment_Analysis,
    YT_Organize_Column,
    RDT_Organize_Column
)

import ETL_Functions.Load_Functions



# File paths for video IDs and Reddit post URLs
VIDEO_IDS_FILE = 'src/resources/video_ids.txt'
REDDIT_POSTS_FILE = 'src/resources/redditpost_url.txt'



# Define Utility Functions
def Extract_Comments_from_YouTube(video_id):
    from googleapiclient.discovery import build

    youtube = build('youtube', 'v3', developerKey = YOUTUBE_API_KEY)

    return Extract_YT_Comments(video_id, youtube)

def Extract_Comments_from_Reddit(post_url):
    import praw

    reddit = praw.Reddit(
        client_id = REDDIT_CLIENT_ID,
        client_secret = REDDIT_CLIENT_SECRET,
        user_agent = REDDIT_USER_AGENT
    )

    return Extract_Reddit_Comments(post_url, reddit)

def Transform_YouTube_Comments(YT_comments):
    YT_comments = YT_Convert_Emoji_to_Text(YT_comments)
    YT_comments = YT_HandleHTML(YT_comments)

    YT_dataframe = YTcomments_to_Dataframe(YT_comments)
    YT_dataframe = YTcolumn_to_string(YT_dataframe)
    YT_dataframe = remove_first_character_in_username(YT_dataframe)

    # Sentiment Analysis
    YT_dataframe = Sentiment_Analysis(YT_dataframe)
    YT_dataframe = YT_Organize_Column(YT_dataframe)

    return YT_dataframe

def Transform_Reddit_Comments(RDT_comments):
    RDT_comments = RDT_Convert_Emoji_to_Text(RDT_comments)
    RDT_comments = RDT_HandleHTML(RDT_comments)

    RDT_dataframe = RDTcomments_to_Dataframe(RDT_comments)
    RDT_dataframe = RDTcolumn_to_string(RDT_dataframe)
        
    # Sentiment Analysis
    RDT_dataframe = Sentiment_Analysis(RDT_dataframe)
    RDT_dataframe = RDT_Organize_Column(RDT_dataframe)

    return RDT_dataframe

def Combine_Dataframes(YT_df, RDT_df):
    Combined_Dataframe = pd.concat([YT_df, RDT_df], ignore_index = True)

    return Combined_Dataframe



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
    
    # Task: Get the next YouTube video ID
    get_video_id_task = PythonOperator(
        task_id = 'Get_Next_Video_ID',
        python_callable = Get_Next_Video_ID
    )

    # Task: Get the next Reddit post URL
    get_reddit_url_task = PythonOperator(
        task_id = 'Get_Next_Reddit_Post_URL',
        python_callable = Get_Next_Reddit_Post_URL
    )



    # Task: Extract YouTube comments
    extract_youtube_task = PythonOperator(
        task_id = 'Extract_Comments_from_YouTube',
        python_callable = Extract_Comments_from_YouTube,
        op_args = ["{{ task_instance.xcom_pull(task_ids='Get_Next_Video_ID') }}"]
    )
    
    # Task: Extract Reddit comments
    extract_reddit_task = PythonOperator(
        task_id = 'Extract_Comments_from_Reddit',
        python_callable = Extract_Comments_from_Reddit,
        op_args = ["{{ task_instance.xcom_pull(task_ids='Get_Next_Reddit_Post_URL') }}"]
    )
    


    # Task: Transform YouTube comments
    transform_youtube_task = PythonOperator(
        task_id = 'Transform_YouTube_Comments',
        python_callable = Transform_YouTube_Comments,
        op_args = ["{{ task_instance.xcom_pull(task_ids='Extract_Comments_from_YouTube') }}"]
    )
    
    # Task: Transform Reddit comments
    transform_reddit_task = PythonOperator(
        task_id = 'Transform_Reddit_Comments',
        python_callable = Transform_Reddit_Comments,
        op_args = ["{{ task_instance.xcom_pull(task_ids='Extract_Comments_from_Reddit') }}"]
    )
    


    # Task: Combine YouTube and Reddit dataframes
    combine_task = PythonOperator(
        task_id = 'Combine_Dataframes',
        python_callable = Combine_Dataframes,
        op_args = [
            "{{ task_instance.xcom_pull(task_ids='Transform_YouTube_Comments') }}",
            "{{ task_instance.xcom_pull(task_ids='Transform_Reddit_Comments') }}"
        ]
    )



    # Set Task Dependencies
    [get_video_id_task, get_reddit_url_task] >> [extract_youtube_task, extract_reddit_task] >> [transform_youtube_task, transform_reddit_task] >> combine_task